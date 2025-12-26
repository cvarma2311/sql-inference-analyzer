from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable

import sqlparse

from .registry_loader import load_mandatory_constraints, load_schema_registry


SQL_KEYWORDS = {
    "select",
    "from",
    "where",
    "group",
    "by",
    "order",
    "and",
    "or",
    "as",
    "on",
    "join",
    "inner",
    "left",
    "right",
    "full",
    "between",
    "in",
    "not",
    "is",
    "null",
    "distinct",
    "case",
    "when",
    "then",
    "else",
    "with",
    "end",
    "limit",
    "offset",
    "round",
    "coalesce",
    "sum",
    "count",
    "min",
    "max",
    "avg",
    "numeric",
    "date",
}


@dataclass
class ValidationResult:
    valid: bool
    errors: list[str]


class SqlValidator:
    def __init__(self) -> None:
        schema = load_schema_registry()
        self.allowed_tables = set(schema.get("tables", {}).keys())
        self.allowed_columns = {
            col
            for table in schema.get("tables", {}).values()
            for col in table.get("allowed_columns", [])
        }
        self.allowed_aliases = {"actual_tmt", "target_tmt", "gap_tmt"}
        self.mandatory_constraints = load_mandatory_constraints()

    def validate(self, sql: str) -> ValidationResult:
        errors: list[str] = []
        sql_stripped = sql.strip()

        if not (
            sql_stripped.lower().startswith("select")
            or sql_stripped.lower().startswith("with")
        ):
            errors.append("Query must start with SELECT or WITH.")

        if not self._contains_mandatory_constraints(sql_stripped):
            errors.append("Missing mandatory constraints.")

        used_tables = self._extract_tables(sql_stripped)
        cte_tables = self._extract_cte_names(sql_stripped)
        if not used_tables:
            errors.append("No table found in query.")
        else:
            disallowed_tables = sorted(
                t for t in used_tables if t not in self.allowed_tables and t not in cte_tables
            )
            if disallowed_tables:
                errors.append(f"Disallowed tables: {disallowed_tables}")

        disallowed_columns = sorted(
            c
            for c in self._extract_columns(sql_stripped)
            if c not in self.allowed_columns and c not in self.allowed_aliases
        )
        if disallowed_columns:
            errors.append(f"Disallowed columns: {disallowed_columns}")

        if self._uses_aggregation(sql_stripped) and not re.search(r"\bgroup\s+by\b", sql_stripped, re.IGNORECASE):
            errors.append("Aggregation requires GROUP BY.")

        if not self._has_bounded_date_filter(sql_stripped):
            errors.append("Missing bounded date filter (BETWEEN on date column or fiscal_year filter).")

        return ValidationResult(valid=not errors, errors=errors)

    def _contains_mandatory_constraints(self, sql: str) -> bool:
        normalized_sql = re.sub(r"\s+", "", sql).lower()
        normalized_constraints = re.sub(r"\s+", "", self.mandatory_constraints).lower()
        return normalized_constraints in normalized_sql

    def _extract_tables(self, sql: str) -> set[str]:
        tables = set()
        pattern = re.compile(
            r"\b(from|join)\s+((?:\"?[A-Za-z0-9_]+\"?\.)?\"?[A-Za-z0-9_]+\"?)",
            re.IGNORECASE,
        )
        for _, table in pattern.findall(sql):
            table_name = table.split(".")[-1]
            tables.add(table_name.replace('"', ""))
        return tables

    def _extract_cte_names(self, sql: str) -> set[str]:
        names = set()
        pattern = re.compile(
            r"\bwith\s+([A-Za-z0-9_]+)\s+as\s*\(|,\s*([A-Za-z0-9_]+)\s+as\s*\(",
            re.IGNORECASE,
        )
        for match in pattern.finditer(sql):
            name = match.group(1) or match.group(2)
            if name:
                names.add(name)
        return names

    def _extract_columns(self, sql: str) -> set[str]:
        columns = set()
        cte_names = self._extract_cte_names(sql)
        aliases = self._extract_table_aliases(sql)
        parsed = sqlparse.parse(sql)
        for statement in parsed:
            for token in statement.flatten():
                if token.ttype is None:
                    continue
                value = token.value
                if value.isidentifier() and value.lower() not in SQL_KEYWORDS:
                    if value in self.allowed_tables:
                        continue
                    if value in cte_names:
                        continue
                    if value in aliases:
                        continue
                    if value.lower() == "public":
                        continue
                    columns.add(value)
                if value.startswith("\"") and value.endswith("\""):
                    unquoted = value.strip('"')
                    if unquoted in self.allowed_tables:
                        continue
                    if unquoted in cte_names:
                        continue
                    if unquoted in aliases:
                        continue
                    if unquoted.lower() == "public":
                        continue
                    columns.add(unquoted)
        return columns

    def _extract_table_aliases(self, sql: str) -> set[str]:
        aliases = set()
        pattern = re.compile(
            r"\b(from|join)\s+([A-Za-z0-9_\"\.]+)\s+([A-Za-z0-9_]+)\b",
            re.IGNORECASE,
        )
        for match in pattern.finditer(sql):
            alias = match.group(3)
            if alias and alias.lower() not in SQL_KEYWORDS:
                aliases.add(alias)
        return aliases

    def _uses_aggregation(self, sql: str) -> bool:
        return bool(re.search(r"\b(sum|count|min|max|avg)\s*\(", sql, re.IGNORECASE))

    def _has_bounded_date_filter(self, sql: str) -> bool:
        date_columns = ["day_id", "year_monthname", "invoice_dt"]
        for col in date_columns:
            quoted_pattern = rf'"{col}"\s*(::date)?\s*between\b'
            unquoted_pattern = rf'\b{col}\b\s*(::date)?\s*between\b'
            if re.search(quoted_pattern, sql, re.IGNORECASE) or re.search(
                unquoted_pattern, sql, re.IGNORECASE
            ):
                return True
        fiscal_patterns = [
            r'"fiscal_year"\s*(=|in|between)\s*',
            r'\bfiscal_year\b\s*(=|in|between)\s*',
        ]
        return any(re.search(pattern, sql, re.IGNORECASE) for pattern in fiscal_patterns)
