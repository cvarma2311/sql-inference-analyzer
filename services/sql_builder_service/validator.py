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
    "end",
    "limit",
    "offset",
    "round",
    "sum",
    "count",
    "min",
    "max",
    "avg",
    "numeric",
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
        self.mandatory_constraints = load_mandatory_constraints()

    def validate(self, sql: str) -> ValidationResult:
        errors: list[str] = []
        sql_stripped = sql.strip()

        if not sql_stripped.lower().startswith("select"):
            errors.append("Query must start with SELECT.")

        if not self._contains_mandatory_constraints(sql_stripped):
            errors.append("Missing mandatory constraints.")

        used_tables = self._extract_tables(sql_stripped)
        if not used_tables:
            errors.append("No table found in query.")
        else:
            disallowed_tables = sorted(t for t in used_tables if t not in self.allowed_tables)
            if disallowed_tables:
                errors.append(f"Disallowed tables: {disallowed_tables}")

        disallowed_columns = sorted(c for c in self._extract_columns(sql_stripped) if c not in self.allowed_columns)
        if disallowed_columns:
            errors.append(f"Disallowed columns: {disallowed_columns}")

        if self._uses_aggregation(sql_stripped) and not re.search(r"\bgroup\s+by\b", sql_stripped, re.IGNORECASE):
            errors.append("Aggregation requires GROUP BY.")

        if not self._has_bounded_date_filter(sql_stripped):
            errors.append("Missing bounded date filter (BETWEEN on date column).")

        return ValidationResult(valid=not errors, errors=errors)

    def _contains_mandatory_constraints(self, sql: str) -> bool:
        normalized_sql = re.sub(r"\s+", "", sql).lower()
        normalized_constraints = re.sub(r"\s+", "", self.mandatory_constraints).lower()
        return normalized_constraints in normalized_sql

    def _extract_tables(self, sql: str) -> set[str]:
        tables = set()
        pattern = re.compile(r"\b(from|join)\s+([\w\"]+)", re.IGNORECASE)
        for _, table in pattern.findall(sql):
            tables.add(table.replace('"', ""))
        return tables

    def _extract_columns(self, sql: str) -> set[str]:
        columns = set()
        parsed = sqlparse.parse(sql)
        for statement in parsed:
            for token in statement.flatten():
                if token.ttype is None:
                    continue
                value = token.value
                if value.isidentifier() and value.lower() not in SQL_KEYWORDS:
                    columns.add(value)
                if value.startswith("\"") and value.endswith("\""):
                    columns.add(value.strip('"'))
        return columns

    def _uses_aggregation(self, sql: str) -> bool:
        return bool(re.search(r"\b(sum|count|min|max|avg)\s*\(", sql, re.IGNORECASE))

    def _has_bounded_date_filter(self, sql: str) -> bool:
        date_columns = ["day_id", "year_monthname", "invoice_dt"]
        for col in date_columns:
            if re.search(rf"\b{col}\b\s*(::date)?\s*between\b", sql, re.IGNORECASE):
                return True
        return False
