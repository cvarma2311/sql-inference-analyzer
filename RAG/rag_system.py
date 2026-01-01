"""
ENHANCED RAG SYSTEM (SentenceTransformer compatible)
Compatible with older sentence-transformers versions
pgvector 0.8.1 safe
"""

import logging
import json
import re
import os
from typing import List, Dict, Optional
from difflib import SequenceMatcher
from datetime import datetime

from sentence_transformers import SentenceTransformer, CrossEncoder
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s: %(message)s")


class EnhancedSQLRAGSystem:

    def __init__(self, training_data_module, sql_validator):
        # DO NOT pass normalize_embeddings here
        self.model = SentenceTransformer("all-MiniLM-L6-v2")
        self.cross_encoder = CrossEncoder("cross-encoder/ms-marco-MiniLM-L-6-v2")

        self.training_data_module = training_data_module
        self.validator = sql_validator

        self.qa_pairs = training_data_module.TRAINING_QA_PAIRS
        self.sql_rules = training_data_module.SQL_RULES
        self.question_synonyms = training_data_module.QUESTION_SYNONYMS
        self.column_synonyms = training_data_module.COLUMN_SYNONYMS

        self.engine = sql_validator.engine
        self.Session = sessionmaker(bind=self.engine)

        self.success_cache_file = "success_cache.json"
        self.success_cache = self._load_success_cache()

        self._initialize_schema()

        self.documents = self._build_documents_from_training()
        self._prepare_vector_db()

        logging.info(" EnhancedSQLRAGSystem initialized successfully")

    # ------------------------------------------------------------------
    # SCHEMA
    # ------------------------------------------------------------------
    def _initialize_schema(self):
        self.demo_schema = {
            "vts_alert_history": "tl_number, violation_type[], vts_end_datetime, location_name",
            "vts_truck_master": "truck_no, transporter_name, whether_truck_blacklisted",
            "tt_risk_score": "tt_number, risk_score"
        }

    # ------------------------------------------------------------------
    # VECTOR DATABASE
    # ------------------------------------------------------------------
    def _prepare_vector_db(self):
        logging.info("Preparing pgvector database...")

        with self.engine.begin() as conn:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
            conn.execute(text("DROP TABLE IF EXISTS rag_documents CASCADE"))

            conn.execute(text("""
                CREATE TABLE rag_documents (
                    id TEXT PRIMARY KEY,
                    text TEXT NOT NULL,
                    metadata JSONB,
                    embedding vector(384)
                )
            """))

            texts = [d["text"] for d in self.documents]

            # Normalize embeddings MANUALLY
            embeddings = self.model.encode(texts)
            embeddings = embeddings / (embeddings**2).sum(axis=1, keepdims=True) ** 0.5

            for doc, emb in zip(self.documents, embeddings):
                conn.execute(text("""
                    INSERT INTO rag_documents (id, text, metadata, embedding)
                    VALUES (:id, :text, :metadata, CAST(:embedding AS vector))
                """), {
                    "id": doc["id"],
                    "text": doc["text"],
                    "metadata": json.dumps(doc["metadata"]),
                    "embedding": emb.tolist()
                })

            conn.execute(text("""
                CREATE INDEX rag_documents_embedding_idx
                ON rag_documents
                USING hnsw (embedding vector_cosine_ops)
                WITH (m = 16, ef_construction = 64)
            """))

        logging.info(" pgvector table populated")

    # ------------------------------------------------------------------
    # RETRIEVAL
    # ------------------------------------------------------------------
    def retrieve_relevant_context(self, question: str, top_k: int = 5) -> List[Dict]:
        normalized = self.normalize_question(question)

        query_emb = self.model.encode(normalized)
        query_emb = query_emb / (query_emb @ query_emb) ** 0.5

        session = self.Session()
        try:
            session.execute(text("SET hnsw.ef_search = 100"))

            rows = session.execute(text("""
                SELECT id, text, metadata,
                       1 - (embedding <=> CAST(:embedding AS vector)) AS score
                FROM rag_documents
                ORDER BY embedding <=> CAST(:embedding AS vector)
                LIMIT :limit
            """), {
                "embedding": query_emb.tolist(),
                "limit": top_k * 5
            }).fetchall()

            docs = []
            for r in rows:
                meta = r.metadata if isinstance(r.metadata, dict) else json.loads(r.metadata or "{}")
                docs.append({
                    "id": r.id,
                    "text": r.text,
                    "metadata": meta,
                    "score": float(r.score)
                })

            return self._rerank(question, docs, top_k)

        finally:
            session.close()

    # ------------------------------------------------------------------
    # RERANK
    # ------------------------------------------------------------------
    def _rerank(self, question: str, docs: List[Dict], top_k: int):
        if not docs:
            return []

        pairs = [[question, d["text"]] for d in docs]
        scores = self.cross_encoder.predict(pairs)

        for d, s in zip(docs, scores):
            d["rerank_score"] = float(s)

        return sorted(docs, key=lambda x: x["rerank_score"], reverse=True)[:top_k]

    # ------------------------------------------------------------------
    # DOCUMENT BUILDING
    # ------------------------------------------------------------------
    def _build_documents_from_training(self) -> List[Dict]:
        docs = []
        idx = 0

        schema_docs = getattr(self.training_data_module, "SCHEMA_DESCRIPTIONS", {})
        for table, info in schema_docs.items():
            docs.append({
                "id": f"schema_{idx}",
                "text": f"Table {table}: {info.get('description', '')}",
                "metadata": {"type": "schema", "table": table}
            })
            idx += 1

        for i, qa in enumerate(self.qa_pairs):
            docs.append({
                "id": f"qa_{i}",
                "text": f"QUESTION: {qa['question']}\nSQL: {qa['sql']}",
                "metadata": {"type": "example"}
            })

        docs.append({
            "id": "rules",
            "text": self.sql_rules.get("business_rules", ""),
            "metadata": {"type": "rules"}
        })

        return docs

    # ------------------------------------------------------------------
    # CACHE
    # ------------------------------------------------------------------
    def normalize_question(self, q: str) -> str:
        q = q.lower().strip().rstrip("?")
        for pat, rep in self.question_synonyms.items():
            q = re.sub(pat, rep, q)
        return q

    def _load_success_cache(self):
        """
        Load successful query cache safely.
        Handles empty, corrupted, or missing JSON.
        """
        if not os.path.exists(self.success_cache_file):
            return {}

        try:
            with open(self.success_cache_file, "r") as f:
                content = f.read().strip()
                if not content:
                    logging.warning("Success cache file is empty. Reinitializing.")
                    return {}
                return json.loads(content)

        except json.JSONDecodeError:
            logging.error("Success cache file is corrupted. Resetting cache.")
            return {}

        except Exception as e:
            logging.error(f"Failed to load success cache: {e}")
            return {}


    def cache_successful_query(self, question: str, sql: str, source: str):
        self.success_cache[self.normalize_question(question)] = {
            "sql": sql,
            "source": source,
            "time": datetime.now().isoformat()
        }
        json.dump(self.success_cache, open(self.success_cache_file, "w"), indent=2)

    def find_in_cache(self, question: str, threshold: float = 0.9) -> Optional[str]:
        nq = self.normalize_question(question)
        if nq in self.success_cache:
            return self.success_cache[nq]["sql"]

        best = (0, None)
        for k, v in self.success_cache.items():
            score = SequenceMatcher(None, nq, k).ratio()
            if score > best[0]:
                best = (score, v["sql"])

        return best[1] if best[0] >= threshold else None

    def invalidate_cache_entry(self, question: str):
        nq = self.normalize_question(question)
        if nq in self.success_cache:
            del self.success_cache[nq]
            json.dump(self.success_cache, open(self.success_cache_file, "w"), indent=2)
