/**
 * SQLite (read‑only) → PostgreSQL (empty) migration using Sequelize.
 * v4 — robust PG-side type classification + sanitization
 *
 * - Builds tables in PG, then uses PG's actual types (describeTable) to classify columns
 * - Sanitizes before insert:
 *    • Dates: epoch s/ms → timestamptz; "" → NULL
 *    • Booleans: 0/1/true/false/yes/no → boolean; "" → NULL
 *    • Numerics (int/float/double/decimal): ""/whitespace/non-numeric → NULL
 * - Optional on-error row-by-row fallback to identify the exact bad row/column
 *
 * ENV:
 *   SQLITE_PATH=./source.sqlite
 *   PG_URI=postgres://user:pass@localhost:5432/yourdb
 *   PG_SCHEMA=public
 *   BATCH_SIZE=5000
 *   ADD_FOREIGN_KEYS=false
 *   INCLUDE_TABLES=
 *   EXCLUDE_TABLES=
 *   TREAT_EMPTY_STRING_AS_NULL=true
 *   DEBUG_ON_ERROR=false   # set to true to fall back to row-by-row insert on batch failure
 */
