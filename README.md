Reproducible SQL benchmarks for PostgreSQL deletion techniques.

- Main script: `tests.sql`
- What it measures: runtime, WAL delta/volume, dead tuples (and optionally replica lag)

Run:
- `psql -f tests.sql`

There are also examples about different DML based deletion techniques that you can use in production.
