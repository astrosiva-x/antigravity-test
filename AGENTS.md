# AGENTS.md

Guidance for coding agents working in this repository.

## Repository purpose

This repository contains example Airflow DAGs that demonstrate:

- TaskFlow API usage
- idempotent task design
- correct date handling for scheduled runs, manual runs, and backfills

## Repository map

- `best_practices_dag.py`
  - Main "best practices" example.
  - Includes both a bad example (`datetime.now()`) and good examples (`logical_date`, `ds`).
- `example_idempotent_dag.py`
  - Simpler idempotency-focused demo DAG.

## What to optimize for

1. Keep examples educational and easy to read.
2. Preserve deterministic behavior across retries and backfills.
3. Prefer minimal, targeted edits over broad refactors.

## Airflow-specific rules

- Do not use `datetime.now()` for business-date logic in production-path tasks.
  - Use Airflow context values such as `logical_date`, `ds`, or `ds_nodash`.
- Keep write operations idempotent.
  - Prefer overwrite/upsert semantics over append-only behavior.
- Ensure tasks are safe to retry.
- Keep manual runs and backfills working unless a change explicitly narrows scope.
- Avoid changing `dag_id` values unless the task explicitly asks for it.

## Code style and structure

- Use clear task and function names that explain intent.
- Keep docstrings concise but useful, especially in demo tasks.
- Favor straightforward Python over clever abstractions.
- Add short comments only where behavior may be non-obvious.

## Validation checklist

Run this before finalizing changes:

```bash
python -m py_compile best_practices_dag.py example_idempotent_dag.py
```

If an Airflow environment is available, also verify DAG importability in that environment.

## Change boundaries

- Do not remove the intentionally "bad practice" demo sections unless requested.
- Do not introduce new heavy dependencies unless required by the task.
- Keep changes contained to files relevant to the request.

## Commit guidance

- Use clear commit messages that describe the behavior change.
- Mention idempotency/date-handling impact in commit messages when relevant.
