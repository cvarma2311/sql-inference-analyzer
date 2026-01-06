

import os
import re 
import logging
import dotenv
from sqlalchemy import create_engine, text, inspect
from typing import Tuple, Optional, Dict, List
try:
    from fuzzywuzzy import process as fuzzy_process
except ImportError:
    fuzzy_process = None
    
from orchestrator.dbconnector import credential_loader

class EnhancedSQLValidator:
    """Enhanced validator with smart error detection and intelligent suggestions"""
    
    def __init__(self, sql_rules):
        self.sql_rules = sql_rules
        self.engine = self.get_db_engine()
        self.real_schema = self._extract_real_schema()
        self.allowed_tables = set(self.real_schema.keys())
        self.allowed_columns = self._extract_allowed_columns_from_db()
        
        # NEW: Build intelligent column suggestions
        self.validation_cache = {}
        self.common_errors = self._build_error_patterns()
        self.error_frequency = {}
        
        # NEW: Build intelligent column suggestions
        self.column_similarity_index = self._build_column_similarity_index()
        
        logging.info(f" Enhanced Validator: {len(self.allowed_tables)} tables, {len(self.allowed_columns)} columns")
        
    def _build_column_similarity_index(self) -> Dict:
        """Build an index for intelligent column name suggestions"""
        similarity_index = {}
        
        # Common misspellings and alternatives
        common_mappings = {
            'vehicle': ['tt_number', 'tl_number', 'truck_no', 'vehicle_number', 'trucknumber'],
            'blacklist': ['whether_truck_blacklisted'],
            'blacklisted': ['whether_truck_blacklisted'], 
            # 'blocked' is intentionally omitted. It refers to a status in the 'alerts' table (vehicle_unblocked_date IS NULL)
            # and is handled by deterministic synonym rules, not column similarity.
            'transporter': ['transporter_name', 'transporter_code'],
            'location': ['location_name', 'vehicle_location'],
            'datetime': ['created_at', 'vts_end_datetime', 'createdat'],
            'invoice': ['invoice_number', 'invoicenumber', 'invoice_no'],
            'risk': ['risk_score'],
        }
        
        for concept, columns in common_mappings.items():
            similarity_index[concept] = columns
        
        return similarity_index
        
    def _build_error_patterns(self) -> Dict:
        """Enhanced error patterns with more coverage"""
        return {
            'missing_table': {
                'pattern': r'missing FROM-clause entry for table "(\w+)"',
                'fix': 'Add table to FROM clause or remove references',
                'severity': 'high'
            },
            'ambiguous_column': {
                'pattern': r'column reference "(\w+)" is ambiguous',
                'fix': 'Prefix with table alias (e.g., table.column)',
                'severity': 'high'
            },
            'undefined_column': {
                'pattern': r'column "(\w+)" does not exist',
                'fix': 'Check column name against schema',
                'severity': 'high'
            },
            'invalid_operator': {
                'pattern': r'operator does not exist.*character varying.*\+',
                'fix': 'Use || for string concatenation, not +',
                'severity': 'medium'
            },
            'group_by_error': {
                'pattern': r'must appear in the GROUP BY clause',
                'fix': 'Add all non-aggregated SELECT columns to GROUP BY',
                'severity': 'high'
            },
            'aggregate_in_join': {
                'pattern': r'aggregate functions are not allowed in JOIN conditions',
                'fix': 'Move the aggregate condition to a HAVING clause or use a CTE to pre-calculate.',
                'severity': 'high'
            },
            'timestamp_vs_int': {
                'pattern': r'operator does not exist: timestamp.*integer',
                'fix': 'Do not compare timestamp with integer. Use date < CURRENT_DATE - INTERVAL \'X days\'.',
                'severity': 'high'
            },
            'wrong_table_reference': {
                'pattern': r'relation "(\w+)" does not exist',
                'fix': 'Check table name spelling and ensure table exists',
                'severity': 'high'
            },
            'malformed_array': {
                'pattern': r'malformed array literal',
                'fix': 'You are treating a string as an array or vice versa. Use array_to_string() or ARRAY[] constructor.',
                'severity': 'high'
            }
        }
        
    def _extract_real_schema(self):
        """Extract ACTUAL schema from database"""
        schema = {}
        inspector = inspect(self.engine)
        
        tables = ['vts_alert_history', 'vts_truck_master', 'vts_ongoing_trips', 'alerts',
                  'vts_tripauditmaster', 'tt_risk_score', 'transporter_risk_score', 'completed_trips_risk_score']
        
        for table in tables:
            try:
                columns = inspector.get_columns(table)
                schema[table] = [col['name'] for col in columns]
                logging.info(f"   {table}: {len(columns)} columns")
            except Exception as e:
                logging.error(f"   {table}: {e}")
                
        return schema
    
    def _extract_allowed_columns_from_db(self):
        """Extract all allowed columns from actual database"""
        columns = set()
        for table_cols in self.real_schema.values():
            columns.update(table_cols)
        return columns
    
    def validate_sql(self, sql: str, bypass_forbidden_joins: bool = False) -> Tuple[bool, str]:
        """Enhanced validation with learning from errors"""
        if not sql or sql.strip() == "":
            return False, "Empty SQL query"
        
        sql_hash = hash(sql)
        if sql_hash in self.validation_cache:
            return self.validation_cache[sql_hash]
        
        # 1. Quick pre-validation checks for common, easy-to-spot errors
        col_valid, col_error = self.validate_column_references(sql)
        if not col_valid:
            self._track_error('column_reference_error')
            result = (False, col_error)
            self.validation_cache[sql_hash] = result
            return result
            
        sql_lower = sql.lower()
        
        # 2. Security checks for destructive operations
        dangerous_ops = ["delete", "update", "insert", "drop", "alter", "truncate"]
        if any(op in sql_lower for op in dangerous_ops):
            result = (False, "Dangerous SQL operation detected")
            self.validation_cache[sql_hash] = result
            return result
        
        # 3. Pre-validation for common syntax issues (e.g., trailing commas)
        pre_check_result = self._pre_validate_sql(sql)
        if not pre_check_result["valid"]:
            self._track_error('pre_validation_error')
            result = (False, pre_check_result["error"])
            self.validation_cache[sql_hash] = result
            return result
        
        # NEW: Forbidden Join Check based on DATA_MODEL
        if not bypass_forbidden_joins:
            is_forbidden, forbidden_error = self._check_forbidden_joins(sql)
            if is_forbidden:
                self._track_error('forbidden_join_error')
                result = (False, forbidden_error)
                self.validation_cache[sql_hash] = result
                return result
            
        # Temporal Logic Checks
        temporal_valid, temporal_error = self._check_temporal_sanity(sql)
        if not temporal_valid:
            self._track_error('temporal_logic_error')
            return (False, temporal_error)

        # Execute validation
        # Execute validation - try EXPLAIN first, then LIMIT 1 as fallback
        try:
            with self.engine.connect() as conn:
                test_sql = sql if 'limit' in sql.lower() else f"EXPLAIN {sql}"
                conn.execute(text(test_sql))
                result = (True, "SQL is valid and executable")
                self.validation_cache[sql_hash] = result
                return result
            try:
                with self.engine.connect() as conn:
                    conn.execute(text(f"EXPLAIN {sql}"))
            except Exception:
                # If EXPLAIN fails (e.g., for some UNION queries), try executing with LIMIT 1 as a final check.
                with self.engine.connect() as conn:
                    # Add LIMIT 1 to prevent fetching large datasets during validation
                    if "limit" not in sql.lower():
                        if sql.strip().endswith(';'):
                            sql = sql.strip()[:-1] + " LIMIT 1;"
                        else:
                            sql += " LIMIT 1"
                    conn.execute(text(sql))
            
            result = (True, "SQL is valid and executable")
            self.validation_cache[sql_hash] = result
            return result
        except Exception as e:
            error_msg = str(e)
            self._track_error('execution_error')
            detailed_error = self._analyze_error(error_msg, sql)
            result = (False, detailed_error)
            self.validation_cache[sql_hash] = result
            return result
    
    def _check_temporal_sanity(self, sql: str) -> Tuple[bool, str]:
        """Check for logical temporal errors (e.g. start > end, future dates)"""
        sql_lower = sql.lower()
        
        # Check 1: Start date greater than End date in WHERE clause
        # Matches patterns like: date > '2023-01-01' AND date < '2022-01-01'
        # This is a basic heuristic check
        if "between" in sql_lower:
            # Regex to find BETWEEN 'date1' AND 'date2'
            matches = re.findall(r"between\s+'(\d{4}-\d{2}-\d{2})'\s+and\s+'(\d{4}-\d{2}-\d{2})'", sql_lower)
            for start, end in matches:
                if start > end:
                    return False, f" Temporal Logic Error: Start date '{start}' is after end date '{end}'."
        
        return True, ""

    def _check_forbidden_joins(self, sql: str) -> Tuple[bool, str]:
        """Check for forbidden table combinations based on DATA_MODEL."""
        sql_lower = sql.lower()
        # Find all table names mentioned in the query
        detected_tables = {table for table in self.allowed_tables if re.search(r'\b' + table + r'\b', sql_lower)}

        if len(detected_tables) < 2:
            return False, "" # Not a join, so no forbidden join is possible

        forbidden_rules = self.sql_rules.get("DATA_MODEL", {}).get("forbidden_joins", [])
        for rule in forbidden_rules:
            if rule.issubset(detected_tables):
                # --- FIX: Allow the join ONLY IF it is correctly bridged by the master table ---
                if "vts_truck_master" in detected_tables:
                    continue
                return True, f" Forbidden Join: Joining tables '{' and '.join(rule)}' directly is not allowed. You must join through a master table like vts_truck_master."

        return False, ""

    def _track_error(self, error_type: str):
        """Track error frequency for learning"""
        self.error_frequency[error_type] = self.error_frequency.get(error_type, 0) + 1
    
    def _analyze_error(self, error: str, sql: str) -> str:
        """Enhanced error analysis with intelligent suggestions"""
        for error_type, pattern_data in self.common_errors.items():
            if re.search(pattern_data['pattern'], error):
                match = re.search(pattern_data['pattern'], error)
                if match:
                    problem = match.group(1) if match.groups() else 'unknown'
                    
                    # Provide context-specific fix
                    if error_type == 'undefined_column':
                        return self._suggest_column_fix(problem, sql)
                    elif error_type == 'missing_table':
                        return self._suggest_table_fix(problem)
                    elif error_type == 'wrong_table_reference':
                        return self._suggest_table_fix(problem, is_wrong_table=True)
                    elif error_type == 'ambiguous_column':
                        return self._suggest_alias_fix(problem, sql)
                    elif error_type == 'group_by_error':
                        return self._suggest_group_by_fix(sql, error)
                    else:
                        return f"{error_type.upper()}: '{problem}' - {pattern_data['fix']}\nOriginal error: {error[:200]}"
        
        return f"SQL execution failed: {error[:300]}"
    
    def _suggest_group_by_fix(self, sql: str, error: str) -> str:
        """
        Intelligently suggest a GROUP BY fix by parsing the SQL to find the exact missing columns.
        """
        # Regex to find columns in SELECT that are NOT inside an aggregate function
        # This is a simplified pattern but covers many common cases.
        # It looks for words that are not aggregate functions and are not followed by '('.
        try:
            select_clause_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql, re.IGNORECASE | re.DOTALL)
            if not select_clause_match:
                return "Could not parse SELECT clause to suggest a GROUP BY fix."

            select_clause = select_clause_match.group(1)

            # Find all potential column names/aliases in the SELECT clause
            # This pattern finds words that could be columns, ignoring function calls
            potential_cols = re.findall(r'\b([a-zA-Z0-9_]+\.[a-zA-Z0-9_]+|[a-zA-Z0-9_]+)\b(?!\s*\()', select_clause)
            
            # Filter out keywords that are not columns
            keywords_to_ignore = {'as', 'distinct', 'case', 'when', 'then', 'else', 'end'}
            non_agg_cols = {col.split('.')[-1] for col in potential_cols if col.lower() not in keywords_to_ignore}

            group_by_clause_match = re.search(r'GROUP BY\s+(.*?)(?:\s+HAVING|\s+ORDER BY|\s*$)', sql, re.IGNORECASE | re.DOTALL)
            grouped_cols = set()
            if group_by_clause_match:
                grouped_cols_str = group_by_clause_match.group(1)
                grouped_cols = {col.strip().split('.')[-1] for col in grouped_cols_str.split(',')}

            missing_cols = non_agg_cols - grouped_cols
            if missing_cols:
                return f"GROUP BY ERROR: The query is missing columns in the GROUP BY clause.  FIX: Add `{', '.join(sorted(list(missing_cols)))}` to the GROUP BY clause."
        except Exception:
            # Fallback for safety
            return "GROUP BY ERROR: A non-aggregated column in your SELECT list is missing from the GROUP BY clause. Please add it."
        return "GROUP BY ERROR: Every non-aggregated column in SELECT must be in GROUP BY.  FIX: Review your SELECT columns and add missing ones to GROUP BY."
    
    def validate_column_references(self, sql: str) -> Tuple[bool, str]:
        """Enhanced validation for common column reference errors"""
        """Enhanced validation for common column reference errors and hallucinated columns"""
        common_errors = {
            'vah.vehicle_number': 'vah.tl_number',
            'a.device_tamper_count': 'vah.device_tamper_count',
            'a.device_offline_count': 'vah.device_offline_count',
            'vtm.blacklist': 'vtm.whether_truck_blacklisted',
            'blacklisted': 'whether_truck_blacklisted',
            'is_blacklisted': 'whether_truck_blacklisted',
            'violation_types': 'violation_type',
            'capacity': 'capacity_of_the_truck',
        }
        
        for wrong, correct in common_errors.items():
            if re.search(r'\b' + re.escape(wrong) + r'\b', sql, re.IGNORECASE):
                return False, f"Invalid column reference: '{wrong}' -> use '{correct}'"

        sql_lower = sql.lower()
        col_pattern = r'\b([a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*|[a-z_][a-z0-9_]+)(?!\s*\()'
        potential_cols = re.findall(col_pattern, sql_lower)
        
        return True, "Column references OK"
    
        mentioned_columns = set()
        for col in potential_cols:
            mentioned_columns.add(col.split('.')[-1])
        
        keywords = {'select', 'from', 'where', 'join', 'on', 'group', 'by', 'having', 'order', 'limit', 'distinct', 'as', 'and', 'or', 'not', 'in', 'exists', 'between', 'like', 'case', 'when', 'then', 'else', 'end', 'union', 'all', 'desc', 'asc', 'is', 'null', 'lateral', 'unnest'}
        known_aliases = {'vah', 'vtm', 'vot', 'a', 'trs', 'tam', 'md'}
        mentioned_columns = mentioned_columns - keywords - self.allowed_tables - known_aliases
        
        invalid_cols = mentioned_columns - self.allowed_columns
        if invalid_cols:
            first_invalid = sorted(list(invalid_cols))[0]
            suggestion = self._suggest_column_fix(first_invalid, sql)
            return False, suggestion
        
        return True, "Column references seem valid."
    
    def _pre_validate_sql(self, sql: str) -> Dict:
        """Enhanced pre-validation with more checks"""
        
        # Check for trailing commas
        if re.search(r',\s*\n\s*FROM', sql, re.IGNORECASE):
            return {
                "valid": False,
                "error": " Trailing comma before FROM clause. Remove the comma after the last SELECT column."
            }
        
        # Check for string concatenation errors
        if re.search(r"'[^']*'\s*\+\s*", sql) or re.search(r"\s*\+\s*'[^']*'", sql):
            return {
                "valid": False, 
                "error": " Invalid string concatenation. Use || operator instead of +"
            }
        
        return {"valid": True, "error": None}

    def _suggest_column_fix(self, column: str, sql: str = "") -> str:
        """Smart column name suggestions with similarity matching"""
        # Try to find similar columns
        similar = []
        column_lower = column.lower()
        
        # First, check our similarity index
        for concept, columns in self.column_similarity_index.items():
            if concept in column_lower or column_lower in concept:
                similar.extend(columns)
        
        # Then check actual schema columns
        for col in self.allowed_columns:
            if column_lower in col.lower() or col.lower() in column_lower:
                similar.append(col)
        
        # Remove duplicates and limit to top 3
        similar = list(dict.fromkeys(similar))[:3]
        
        if similar:
            return f" Column '{column}' not found.\n Did you mean: {', '.join(similar)}?\n\n" \
                   f"Common fixes:\n" \
                   f"- Check if you're using the correct table alias\n" \
                   f"- Verify the column name in the schema\n" \
                   f"- Make sure you've joined the table containing this column"
        
        # --- NEW: Cross-Table Lookup Logic ---
        # Check if this column exists primarily in another table (especially the master table)
        tables_containing_col = []
        for table, cols in self.real_schema.items():
            if column_lower in [c.lower() for c in cols]:
                tables_containing_col.append(table)
        
        if tables_containing_col:
            # If specifically found in vts_truck_master, give a strong hint
            if 'vts_truck_master' in tables_containing_col:
                return (f" Column '{column}' does NOT exist in the current table(s), but it DOES exist in 'vts_truck_master'.\n"
                        f" FIX: You MUST JOIN 'vts_truck_master' (alias vtm) to your query to filter/select by '{column}'.\n"
                        f" Example: JOIN vts_truck_master vtm ON [current_table_alias].vehicle_id_col = vtm.truck_no")
            
            # General hint for other tables
            return (f" Column '{column}' does NOT exist in the selected table(s), but it was found in: {', '.join(tables_containing_col)}.\n"
                    f" FIX: You need to JOIN one of these tables to access this column.")

        return f"Column '{column}' does not exist in schema.\n Check the DATABASE SCHEMA for valid columns."
    
          
    
    def _suggest_table_fix(self, table_or_alias: str, is_wrong_table: bool = False) -> str:
        """Smart table/alias suggestions"""
        # --- PRODUCTION FIX: Add fuzzy matching for wrong table names ---
        if is_wrong_table:
            try:
                from fuzzywuzzy import process as fuzzy_process
                best_match = fuzzy_process.extractOne(table_or_alias, self.allowed_tables)
                if best_match and best_match[1] > 70:
                    return (f" WRONG_TABLE_REFERENCE: Table '{table_or_alias}' does not exist.\n"
                            f" Did you mean '{best_match[0]}'? Please use valid table names from the schema.")
            except ImportError:
                pass
            return f" WRONG_TABLE_REFERENCE: Table '{table_or_alias}' does not exist in the schema."

        common_aliases = {
            'vah': 'vts_alert_history',
            'vtm': 'vts_truck_master',
            'vot': 'vts_ongoing_trips',
            'a': 'alerts',
            'trs': 'tt_risk_score',
            'tam': 'vts_tripauditmaster'
        }
        
        if table_or_alias in common_aliases:
            table = common_aliases[table_or_alias]
            return f" Add to FROM clause: ... FROM {table} {table_or_alias} ...\n" \
                   f"Or if joining: ... JOIN {table} {table_or_alias} ON [condition] ..."
        
        return f" Alias '{table_or_alias}' not defined. Add table to FROM or JOIN clause"
    
    
    def _suggest_alias_fix(self, column: str, sql: str) -> str:
        """Smart alias suggestions"""
        # Find which tables have this column
        tables_with_col = []
        for table, columns in self.real_schema.items():
            if column in columns:
                tables_with_col.append(table)
        
        if tables_with_col:
            suggestions = []
            table_alias_map = {
                'vts_alert_history': 'vah',
                'vts_truck_master': 'vtm',
                'vts_ongoing_trips': 'vot',
                'alerts': 'a',
                'tt_risk_score': 'trs'
            }
            
            for table in tables_with_col:
                alias = table_alias_map.get(table, table[:3])
                suggestions.append(f"{alias}.{column}")
            
            return f" Column '{column}' is ambiguous (exists in {len(tables_with_col)} tables).\n" \
                   f" Prefix with table alias. Options: {', '.join(suggestions)}"
        
        return f" Column '{column}' is ambiguous.\n Prefix with table alias: table.{column}"

    def get_db_engine(self):
        """Returns the SQLAlchemy engine for the configured database (supports .env and credential_loader)"""
        try:
            # 1. Try to load from local .env in the RAG2 folder for portability
            rag2_dir = os.path.dirname(os.path.abspath(__file__))
            env_path = os.path.join(rag2_dir, ".env")
            
            if os.path.exists(env_path):
                logging.info(f" Loading credentials from local .env: {env_path}")
                dotenv.load_dotenv(dotenv_path=env_path)
            
            # 2. Extract credentials from environment OR fallback to credential_loader
            if os.getenv("DB_HOST"):
                creds = {
                    "host": os.getenv("DB_HOST"),
                    "port": os.getenv("DB_PORT", "5432"),
                    "user": os.getenv("DB_USER"),
                    "password": os.getenv("DB_PASSWORD"),
                    "database": os.getenv("DB_NAME")
                }
                logging.info(" Using database credentials from environment variables")
            else:
                logging.info(" DB_HOST not found in environment. Falling back to framework credential_loader.")
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
                pool_recycle=3600,
            )
            
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logging.info(" Database connection successful")
            return engine
        except Exception as e:
            logging.error(f" Database connection failed: {e}")
            raise
    
    def get_error_stats(self) -> Dict:
        """Return error frequency statistics for analysis"""
        return self.error_frequency.copy()
    
    def get_most_common_errors(self, top_n: int = 5) -> List[Tuple[str, int]]:
        """Get the most common errors for learning"""
        sorted_errors = sorted(self.error_frequency.items(), key=lambda x: x[1], reverse=True)
        return sorted_errors[:top_n]
