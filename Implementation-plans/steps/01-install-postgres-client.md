# Step 01 - Install Postgres Client Libraries

## Goal
Ensure the runtime has PostgreSQL client libraries for SQLAlchemy/psycopg2.

## Inputs
- Target runtime environment (local, VM, container).
- OS package manager or Python environment.

## Actions
- Install PostgreSQL client libraries and headers.
- Install `psycopg2` or `psycopg2-binary` in the Python environment.

## Exit Criteria
- Python can import `psycopg2`.
- A simple connection test can be established using the configured DB credentials.
