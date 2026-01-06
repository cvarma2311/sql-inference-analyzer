import time
import logging
import json
import re
import hashlib
import os
import numpy as np
from datetime import datetime
from typing import List, Dict, Tuple, Optional, Any, Union

from sentence_transformers import SentenceTransformer, CrossEncoder
from sqlalchemy import text, inspect
from sqlalchemy.orm import sessionmaker
from difflib import SequenceMatcher

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s: %(message)s")

class EnhancedSQLRAGSystem:
    """
    ENHANCED RAG SYSTEM (pgvector 0.8.1 + CrossEncoder)
    Maintains compatibility with ProductionSQLGenerator by providing 
    intent classification and parameter extraction logic.
    """
    
    def __init__(self, training_data_module, sql_validator, force_reindex: bool = False):
        # 1. Models
        self.model = SentenceTransformer("all-MiniLM-L6-v2")
        self.cross_encoder = CrossEncoder("cross-encoder/ms-marco-MiniLM-L-6-v2")

        # 2. Data Sources
        self.training_data_module = training_data_module
        self.validator = sql_validator
        self.qa_pairs = training_data_module.TRAINING_QA_PAIRS
        self.sql_rules = training_data_module.SQL_RULES
        
        # --- NEW: Load Learned Data BEFORE creating embeddings ---
        self.learned_data_file = os.path.join(os.path.dirname(__file__), "learned_qa_pairs.json")
        self._load_learned_data()
        
        self.qa_embeddings = self.model.encode([qa['question'] for qa in self.qa_pairs], normalize_embeddings=True)
        self.question_synonyms = training_data_module.QUESTION_SYNONYMS
        self.column_synonyms = training_data_module.COLUMN_SYNONYMS
        
        # 3. DB & Cache Config
        self.engine = sql_validator.engine
        self.Session = sessionmaker(bind=self.engine)
        self.success_cache_file = "semantic_success_cache.json"
        self.success_cache = self._load_success_cache()
        self.cache_embeddings = []
        self.cache_data = []

        # 4. Critical Logic for Generator Compatibility
        self.query_type_keywords = {
            'current_status': ['current', 'status', 'where is', 'right now', 'live', 'ongoing'],
            'historical': ['history', 'past', 'last month', 'yesterday', 'trend'],
            'risk': ['risk score', 'risky', 'risk'],
            'violation': ['violation', 'violations', 'violating'],
            'aggregation': ['total', 'count', 'sum', 'average', 'percentage'],
            'alerts': ['alert', 'alerts', 'alarm', 'severity'],
            'audit': ['swipe', 'emlock', 'audit', 'lock'],
            'transporter': ['transporter', 'carrier', 'performance'],
            'exclusion': ['not repeated', 'but not', 'and not in'],
        }
        self._initialize_templates_and_schema()
        self._rebuild_cache_indices()

        # 5. Initialize Vector Store
        self.documents = self._build_documents_from_training()
        self._prepare_vector_db(force_reindex=force_reindex)

        logging.info(" EnhancedSQLRAGSystem initialized with pgvector backend")

    def _initialize_templates_and_schema(self):
        """Hardcoded metadata used by the LLM Prompt Generator"""
        self.demo_schema = {
            # --- NEW: Add completed_trips_risk_score to the demo schema ---
            "completed_trips_risk_score": "trip_name, risk_score, scheduled_trip_end_datetime, tt_number",
            "vts_alert_history": "tl_number, stoppage_violations_count, route_deviation_count, speed_violation_count, violation_type (text[]), vts_end_datetime, location_name",
            "vts_truck_master": "truck_no, transporter_name, whether_truck_blacklisted, zone, region",
            "alerts": "vehicle_number, alert_section, alert_status, severity, created_at",
            "tt_risk_score": "tt_number, risk_score, total_trips"
        }
        
        self.intent_templates = {
            "vehicle_history": {
                "tables": ["vts_alert_history", "vts_truck_master"],
                "template": "SELECT vah.violation_type, vah.vts_end_datetime FROM vts_alert_history vah WHERE vah.tl_number ILIKE '{vehicle_id}' ORDER BY vah.vts_end_datetime DESC",
                "join_hint": "vts_alert_history.tl_number = vts_truck_master.truck_no"
            },
            "exclusion_logic": {
                "tables": ["vts_alert_history"],
                "template": "SELECT DISTINCT tl_number FROM vts_alert_history WHERE vts_end_datetime >= CURRENT_DATE - INTERVAL '{past_period}' EXCEPT SELECT DISTINCT tl_number FROM vts_alert_history WHERE vts_end_datetime >= CURRENT_DATE - INTERVAL '{recent_period}'",
                "join_hint": "Use a LEFT JOIN with IS NULL or EXCEPT clause for negative queries."
            }
        }

    def _load_learned_data(self):
        """Load dynamically learned QA pairs from disk."""
        if os.path.exists(self.learned_data_file):
            try:
                with open(self.learned_data_file, 'r') as f:
                    learned_data = json.load(f)
                    # Append to the main QA pairs list so they are included in embeddings and docs
                    initial_count = len(self.qa_pairs)
                    self.qa_pairs.extend(learned_data)
                    logging.info(f"  [RAG] Loaded {len(learned_data)} learned examples. Total QA pairs: {len(self.qa_pairs)}")
            except Exception as e:
                logging.error(f"  [RAG] Failed to load learned data: {e}")

    # ------------------------------------------------------------------
    # PGVECTOR OPERATIONS
    # ------------------------------------------------------------------
    def _prepare_vector_db(self, force_reindex: bool = False):
        inspector = inspect(self.engine)
        table_exists = inspector.has_table("rag_documents")

        # --- NEW: Force re-indexing if requested ---
        if force_reindex and table_exists:
            logging.warning("`force_reindex` is True. Dropping and rebuilding the vector store.")
            with self.engine.begin() as conn:
                conn.execute(text("DROP TABLE rag_documents;"))
            table_exists = False # Manually update state after dropping

        if not table_exists:
            logging.info("Creating pgvector table `rag_documents`...")
            with self.engine.begin() as conn:
                conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
                conn.execute(text("""
                    CREATE TABLE rag_documents (
                        id TEXT PRIMARY KEY,
                        text TEXT NOT NULL,
                        metadata JSONB,
                        embedding vector(384)
                    )
                """))
                
                # Deduplicate and Insert
                seen_ids = set()
                unique_docs = []
                for d in self.documents:
                    if d["id"] not in seen_ids:
                        unique_docs.append(d)
                        seen_ids.add(d["id"])

                logging.info(f"  Total documents built: {len(self.documents)}. Unique documents to be indexed: {len(unique_docs)}.")

                embeddings = self.model.encode([d["text"] for d in unique_docs], normalize_embeddings=True)

                for doc, emb in zip(unique_docs, embeddings):
                    conn.execute(text("""
                        INSERT INTO rag_documents (id, text, metadata, embedding)
                        VALUES (:id, :text, :metadata, CAST(:embedding AS vector))
                    """), {
                        "id": doc["id"], "text": doc["text"],
                        "metadata": json.dumps(doc["metadata"]), "embedding": emb.tolist()
                    })
                conn.execute(text("CREATE INDEX ON rag_documents USING hnsw (embedding vector_cosine_ops)"))

    def retrieve_relevant_context(self, question: str, top_k: int = 5) -> List[Dict]:
        normalized = self.normalize_question(question)
        query_emb = self.model.encode(normalized, normalize_embeddings=True)
        q_type = self._classify_query(question)

        session = self.Session()
        try:
            # Use Cosine Distance (<=>) for Vector Search
            rows = session.execute(text("""
                SELECT text, metadata, 1 - (embedding <=> CAST(:embedding AS vector)) AS score
                FROM rag_documents
                ORDER BY embedding <=> CAST(:embedding AS vector) LIMIT :limit
            """), {"embedding": query_emb.tolist(), "limit": top_k * 3}).fetchall()

            docs = [{"text": r.text, "metadata": r.metadata, "score": float(r.score)} for r in rows]
            
            # Application-specific Boosting
            docs = self._boost_negative_intent_docs(question, docs)
            reranked = self._rerank(question, docs, top_k)
            
            # Grounding with Template Guidance
            if q_type in self.intent_templates:
                tpl = self.intent_templates[q_type]
                reranked.insert(0, {
                    "text": f"RECOMMENDED SCHEMA & TEMPLATE:\n{tpl['template']}\nHINT: {tpl['join_hint']}",
                    "metadata": {"type": "template_guidance"},
                    "score": 1.0
                })
            return reranked
        finally:
            session.close()

    def _rerank(self, query: str, docs: List[Dict], top_k: int):
        if not docs: return []
        scores = self.cross_encoder.predict([[query, d["text"]] for d in docs])
        for d, s in zip(docs, scores): d["rerank_score"] = float(s)
        return sorted(docs, key=lambda x: x["rerank_score"], reverse=True)[:top_k]

    def retrieve_gold_standard_query(self, question: str, threshold: float = 0.92) -> Optional[Dict]:
        """
        Finds the most semantically similar 'gold standard' QA pair from the training data.
        This is used for proactive logical plan validation.
        """
        if not hasattr(self, 'qa_embeddings') or len(self.qa_embeddings) == 0:
            return None

        query_emb = self.model.encode(question, normalize_embeddings=True)
        similarities = np.dot(self.qa_embeddings, query_emb)
        best_idx = np.argmax(similarities)

        if similarities[best_idx] >= threshold:
            logging.info(f"  Found a similar gold-standard query (Score: {similarities[best_idx]:.2f}) for logical validation.")
            return self.qa_pairs[best_idx]
        
        logging.info("  No sufficiently similar gold-standard query found for logical validation.")
        return None

    # ------------------------------------------------------------------
    # UTILITIES FOR GENERATOR COMPATIBILITY
    # ------------------------------------------------------------------
    def normalize_question(self, q: str) -> str:
        q = q.lower().strip().rstrip("?")
        for pat, rep in self.question_synonyms.items():
            q = re.sub(pat, rep, q)
        return q

    def _classify_query(self, question: str) -> str:
        q_lower = question.lower()
        if any(kw in q_lower for kw in ["but not", "not generated", "without"]): return "exclusion_logic"
        if re.search(r'\b[A-Z]{2}\d{2}[A-Z0-9]{2,6}\b', q_lower): return "vehicle_timeline"
        if "risk" in q_lower: return "risk"
        if any(kw in q_lower for kw in ["live", "current", "status"]): return "current_status"
        return "general"

    def _extract_all_parameters(self, text: str) -> List[str]:
        """Extracted specifically for cache-substitution logic"""
        params = {
            'vehicle_ids': re.findall(r'\b([A-Z]{2}\d{2}[A-Z0-9]{2,6})\b', text, re.I),
            'numbers': re.findall(r'(?:last|top|at least|>\s*)\s*(\d+)', text.lower()) + re.findall(r'\b(\d+)\b', text)
        }
        all_p = []
        for p_list in params.values(): all_p.extend(p_list)
        return sorted(list(set(all_p)))

    def estimate_complexity(self, question: str) -> int:
        score = 1
        q = question.lower()
        if "join" in q: score += 2
        if "group by" in q or "sum(" in q: score += 2
        if "but not" in q or "not generated" in q: score += 3
        return min(score, 10)

    def _boost_negative_intent_docs(self, question: str, docs: List[Dict]) -> List[Dict]:
        if any(kw in question.lower() for kw in ["not generated", "no alerts", "never had"]):
            for d in docs:
                if "EXCEPT" in d['text'] or "IS NULL" in d['text']:
                    d['score'] += 0.2
        return docs

    # ------------------------------------------------------------------
    # SEMANTIC CACHING
    # ------------------------------------------------------------------
    def _rebuild_cache_indices(self):
        self.cache_embeddings = []
        self.cache_data = []
        for nq, data in self.success_cache.items():
            if 'embedding' in data:
                self.cache_embeddings.append(data['embedding'])
                self.cache_data.append({"nq": nq, "sql": data['sql'], "params": data.get('params', [])})
        if self.cache_embeddings:
            self.cache_embeddings = np.array(self.cache_embeddings)

    def find_in_cache(self, question: str, threshold: float = 0.96) -> Optional[Union[str, Tuple[str, int]]]:
        nq = self.normalize_question(question)
        if nq in self.success_cache:
            logging.info("   Exact Cache Hit")
            return self.success_cache[nq]["sql"]

        if len(self.cache_embeddings) == 0: return None
        
        query_emb = self.model.encode(nq, normalize_embeddings=True)
        similarities = np.dot(self.cache_embeddings, query_emb)
        best_idx = np.argmax(similarities)
        
        if similarities[best_idx] >= threshold:
            cached_entry = self.cache_data[best_idx]
            user_params = self._extract_all_parameters(question)
            
            # Intelligent parameter substitution
            if user_params and cached_entry['params'] and len(user_params) == len(cached_entry['params']):
                adapted_sql = self._smart_parameter_substitution(cached_entry['sql'], cached_entry['params'], user_params)
                logging.info(f"   Semantic Cache Hit ({similarities[best_idx]:.2f}) with Adaptation")
                return adapted_sql, len(user_params)
            
            return cached_entry['sql']
        return None

    def _smart_parameter_substitution(self, sql: str, old_params: List[str], new_params: List[str]) -> str:
        for old, new in zip(old_params, new_params):
            sql = re.sub(re.escape(str(old)), str(new), sql, flags=re.I)
        return sql

    def cache_successful_query(self, question: str, sql: str, source: str):
        # Validate before caching via validator (already handled in Generator, but safe here)
        nq = self.normalize_question(question)
        params = self._extract_all_parameters(question)
        emb = self.model.encode(nq, normalize_embeddings=True).tolist()
        
        self.success_cache[nq] = {
            "sql": sql, "source": source, "params": params,
            "embedding": emb, "time": datetime.now().isoformat()
        }
        with open(self.success_cache_file, "w") as f:
            json.dump(self.success_cache, f, indent=2)
        self._rebuild_cache_indices()

    def invalidate_cache_entry(self, question: str):
        nq = self.normalize_question(question)
        if nq in self.success_cache:
            del self.success_cache[nq]
            with open(self.success_cache_file, "w") as f:
                json.dump(self.success_cache, f, indent=2)
            self._rebuild_cache_indices()
            logging.warning(f" Cache invalidated for: {question}")

    def _load_success_cache(self):
        if os.path.exists(self.success_cache_file):
            try:
                with open(self.success_cache_file, "r") as f:
                    return json.load(f)
            except: return {}
        return {}

    def _build_documents_from_training(self) -> List[Dict]:
        docs = []
        
        def create_doc_id(text: str) -> str:
            # Normalize whitespace to ensure consistent IDs (prevents duplicates from formatting)
            normalized_text = " ".join(text.split())
            return hashlib.sha256(normalized_text.encode('utf-8')).hexdigest()

        schema_docs = getattr(self.training_data_module, "SCHEMA_DESCRIPTIONS", {})
        for table, info in schema_docs.items():
            text_content = f"Table {table}: {info.get('description', '').strip()}".strip()
            docs.append({"id": create_doc_id(text_content), "text": text_content, "metadata": {"type": "schema", "table": table}})
        
        for qa in self.qa_pairs:
            q_clean = qa['question'].strip()
            s_clean = qa['sql'].strip()
            text_content = f"QUESTION: {q_clean}\nSQL: {s_clean}"
            docs.append({"id": create_doc_id(text_content), "text": text_content, "metadata": {"type": "example", "question": q_clean, "sql": s_clean}})
        
        return docs

    def add_learned_example(self, question: str, sql: str, query_type: str = "general"):
        """
        Dynamically adds a verified QA pair to the RAG system and persists it.
        Handles deduplication, in-memory updates, and vector DB insertion.
        """
        # 1. Normalize and Create ID
        q_clean = question.strip()
        s_clean = sql.strip()
        
        # Create a unique ID based on content to prevent duplicates
        content_hash = hashlib.sha256((q_clean + s_clean).encode('utf-8')).hexdigest()
        
        # 2. Check for duplicates in memory
        existing_ids = {d['id'] for d in self.documents}
        
        if content_hash in existing_ids:
            logging.info(f"  [RAG] Data already exists. Skipping insertion for: '{q_clean[:50]}...'")
            return

        logging.info(f"  [RAG] Adding new unique data into RAG: '{q_clean[:50]}...'")

        # 3. Create new document object
        new_doc = {
            "id": content_hash,
            "text": f"QUESTION: {q_clean}\nSQL: {s_clean}",
            "metadata": {"type": "example", "question": q_clean, "sql": s_clean, "query_type": query_type}
        }
        
        # 4. Update In-Memory Stores
        self.documents.append(new_doc)
        self.qa_pairs.append({"question": q_clean, "sql": s_clean, "query_type": query_type})
        
        # Update Embeddings (Incremental)
        # 1. For in-memory gold standard retrieval (Question Only)
        q_emb = self.model.encode(q_clean, normalize_embeddings=True)
        if hasattr(self, 'qa_embeddings') and len(self.qa_embeddings) > 0:
            self.qa_embeddings = np.vstack([self.qa_embeddings, q_emb])
        else:
            self.qa_embeddings = np.array([q_emb])

        # 2. For Vector DB retrieval (Full Text: Question + SQL)
        # This ensures consistency with _prepare_vector_db which embeds d["text"]
        doc_emb = self.model.encode(new_doc["text"], normalize_embeddings=True)

        # 5. Persist to File (learned_qa_pairs.json)
        learned_pairs = []
        if os.path.exists(self.learned_data_file):
            try:
                with open(self.learned_data_file, 'r') as f:
                    learned_pairs = json.load(f)
            except json.JSONDecodeError:
                pass
        
        learned_pairs.append({"question": q_clean, "sql": s_clean, "query_type": query_type})
        with open(self.learned_data_file, 'w') as f:
            json.dump(learned_pairs, f, indent=2)

        # 6. Insert into Vector DB (pgvector)
        try:
            with self.engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO rag_documents (id, text, metadata, embedding)
                    VALUES (:id, :text, :metadata, CAST(:embedding AS vector))
                    ON CONFLICT (id) DO NOTHING
                """), {
                    "id": new_doc["id"], 
                    "text": new_doc["text"],
                    "metadata": json.dumps(new_doc["metadata"]), 
                    "embedding": doc_emb.tolist()
                })
                conn.commit()
            logging.info("  [RAG] Successfully indexed new example into vector store.")
        except Exception as e:
            logging.error(f"  [RAG] Failed to insert into vector DB: {e}")