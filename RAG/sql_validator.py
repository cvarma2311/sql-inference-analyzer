"""
CORRECTED SQL VALIDATOR
No changes needed - your validator is already well-designed
Minor improvements for consistency
"""

import os
import re
import logging
from sqlalchemy import create_engine, text, inspect
from typing import Tuple, Optional, Dict, List

from orchestrator.dbconnector import credential_loader

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

class EnhancedSQLValidator:
    """Enhanced validator with smart error detection"""
    
    def __init__(self, sql_rules):
        self.sql_rules = sql_rules
        self.engine = self.get_db_engine()
        self.real_schema = self._extract_real_schema()
        self.allowed_tables = set(self.real_schema.keys())
        self.allowed_columns = self._extract_allowed_columns_from_db()
        
        self.validation_cache = {}
        self.error_frequency = {}
        
        logging.info(f" Validator: {len(self.allowed_tables)} tables, {len(self.allowed_columns)} columns")
    
    def _extract_real_schema(self):
        """Extract schema from database"""
        schema = {}
        inspector = inspect(self.engine)
        
        tables = ['vts_alert_history', 'vts_truck_master', 'vts_ongoing_trips', 
                  'alerts', 'vts_tripauditmaster', 'tt_risk_score', 'transporter_risk_score']
        
        for table in tables:
            try:
                columns = inspector.get_columns(table)
                schema[table] = [col['name'] for col in columns]
                logging.info(f"   {table}: {len(columns)} columns")
            except Exception as e:
                logging.error(f"  ✗ {table}: {e}")
        
        return schema
    
    def _extract_allowed_columns_from_db(self):
        """Extract all allowed columns"""
        columns = set()
        for table_cols in self.real_schema.values():
            columns.update(table_cols)
        return columns
    
    def validate_sql(self, sql: str, bypass_forbidden_joins: bool = False) -> Tuple[bool, str]:
        """Validate SQL query"""
        if not sql or sql.strip() == "":
            return False, "Empty SQL query"
        
        sql_hash = hash(sql)
        if sql_hash in self.validation_cache:
            return self.validation_cache[sql_hash]
        
        # Pre-validation
        col_valid, col_error = self.validate_column_references(sql)
        if not col_valid:
            result = (False, col_error)
            self.validation_cache[sql_hash] = result
            return result
        
        sql_lower = sql.lower()
        
        # Security check
        dangerous_ops = ["delete", "update", "insert", "drop", "alter", "truncate"]
        if any(op in sql_lower for op in dangerous_ops):
            result = (False, "Dangerous SQL operation detected")
            self.validation_cache[sql_hash] = result
            return result
        
        # Syntax pre-check
        pre_check = self._pre_validate_sql(sql)
        if not pre_check["valid"]:
            result = (False, pre_check["error"])
            self.validation_cache[sql_hash] = result
            return result
        
        # Forbidden joins check
        if not bypass_forbidden_joins:
            is_forbidden, error = self._check_forbidden_joins(sql)
            if is_forbidden:
                result = (False, error)
                self.validation_cache[sql_hash] = result
                return result
        
        # Execute validation
        try:
            with self.engine.connect() as conn:
                conn.execute(text(f"EXPLAIN {sql}"))
            
            result = (True, "SQL is valid")
            self.validation_cache[sql_hash] = result
            return result
        except Exception as e:
            error_msg = str(e)
            detailed_error = self._analyze_error(error_msg, sql)
            result = (False, detailed_error)
            self.validation_cache[sql_hash] = result
            return result
    
    def _check_forbidden_joins(self, sql: str) -> Tuple[bool, str]:
        """Check for forbidden joins"""
        sql_lower = sql.lower()
        detected_tables = {table for table in self.allowed_tables if re.search(r'\b' + table + r'\b', sql_lower)}
        
        if len(detected_tables) < 2:
            return False, ""
        
        forbidden_rules = self.sql_rules.get("DATA_MODEL", {}).get("forbidden_joins", [])
        for rule in forbidden_rules:
            if rule.issubset(detected_tables):
                if "vts_truck_master" in detected_tables:
                    continue
                return True, f"Forbidden join: {' and '.join(rule)} must join through vts_truck_master"
        
        return False, ""
    
    def validate_column_references(self, sql: str) -> Tuple[bool, str]:
        """Validate column references"""
        common_errors = {
            'vah.vehicle_number': 'vah.tl_number',
            'vtm.blacklist': 'vtm.whether_truck_blacklisted',
            'blacklisted': 'whether_truck_blacklisted',
            'is_blacklisted': 'whether_truck_blacklisted',
            'capacity': 'capacity_of_the_truck',
        }
        
        for wrong, correct in common_errors.items():
            if re.search(r'\b' + re.escape(wrong) + r'\b', sql, re.IGNORECASE):
                return False, f"Invalid column: '{wrong}' → use '{correct}'"
        
        return True, "OK"
    
    def _pre_validate_sql(self, sql: str) -> Dict:
        """Pre-validation checks"""
        if re.search(r',\s*\n\s*FROM', sql, re.IGNORECASE):
            return {"valid": False, "error": "Trailing comma before FROM"}
        
        if re.search(r"'[^']*'\s*\+\s*", sql):
            return {"valid": False, "error": "Use || for concatenation, not +"}
        
        return {"valid": True, "error": None}
    
    def _analyze_error(self, error: str, sql: str) -> str:
        """Analyze error"""
        if 'column' in error.lower() and 'does not exist' in error.lower():
            col_match = re.search(r'column "([^"]+)"', error, re.I)
            if col_match:
                return f"Column '{col_match.group(1)}' not found. Check schema."
        
        if 'relation' in error.lower() and 'does not exist' in error.lower():
            return "Table does not exist. Check table name."
        
        if 'ambiguous' in error.lower():
            return "Ambiguous column. Use table alias (e.g., vtm.column_name)"
        
        if 'must appear in the GROUP BY' in error:
            return "GROUP BY error: All non-aggregated SELECT columns must be in GROUP BY"
        
        return f"SQL error: {error[:200]}"
    
    def _suggest_column_fix(self, column: str, sql: str = "") -> str:
        """Suggest column fix"""
        similar = []
        column_lower = column.lower()
        
        for col in self.allowed_columns:
            if column_lower in col.lower() or col.lower() in column_lower:
                similar.append(col)
        
        if similar:
            return f"Column '{column}' not found. Did you mean: {', '.join(similar[:3])}?"
        
        return f"Column '{column}' does not exist in schema"
    
    def _suggest_alias_fix(self, column: str, sql: str) -> str:
        """Suggest alias fix"""
        tables_with_col = [t for t, cols in self.real_schema.items() if column in cols]
        
        if tables_with_col:
            return f"Column '{column}' is ambiguous. Prefix with table alias."
        
        return f"Column '{column}' is ambiguous"
    
    def _suggest_group_by_fix(self, sql: str, error: str) -> str:
        """Suggest GROUP BY fix"""
        return "GROUP BY error: Add all non-aggregated SELECT columns to GROUP BY clause"
    
    def get_db_engine(self):
        """Get database engine"""
        try:
            creds = credential_loader.get_credentials('APP_DB')
            connection_string = (
                f"postgresql+psycopg2://{creds['user']}:{creds['password']}@"
                f"{creds['host']}:{creds['port']}/{creds['database']}"
            )
            engine = create_engine(
                connection_string,
                pool_size=5,
                max_overflow=10,
                pool_pre_ping=True,
                pool_recycle=3600
            )
            
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            logging.info(f" Database connected: {creds['host']}:{creds['port']}/{creds['database']}")
            return engine
        except Exception as e:
            logging.error(f"✗ Database connection failed: {e}")
            raise