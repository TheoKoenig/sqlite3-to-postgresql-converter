#!/usr/bin/env node
'use strict';

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

require('dotenv').config();
const { Sequelize, DataTypes, QueryTypes, literal, Deferrable } = require('sequelize');
const sqlite3 = require('sqlite3');

// ---- Config ----
const SQLITE_PATH = process.env.SQLITE_PATH || './source.sqlite';
const PG_URI = process.env.PG_URI || 'postgres://postgres:postgres@localhost:5432/postgres';
const PG_SCHEMA = process.env.PG_SCHEMA || 'public';
const INCLUDE_TABLES = (process.env.INCLUDE_TABLES || '').split(',').map((s) => s.trim()).filter(Boolean);
const EXCLUDE_TABLES = (process.env.EXCLUDE_TABLES || '').split(',').map((s) => s.trim()).filter(Boolean);
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || '5000', 10);
const ADD_FOREIGN_KEYS = String(process.env.ADD_FOREIGN_KEYS || 'false').toLowerCase() === 'true';
const TREAT_EMPTY_STRING_AS_NULL = String(process.env.TREAT_EMPTY_STRING_AS_NULL || 'true').toLowerCase() === 'true';
const DEBUG_ON_ERROR = String(process.env.DEBUG_ON_ERROR || 'false').toLowerCase() === 'true';

// ---- Sequelize instances ----
const sqlite = new Sequelize({
  dialect: 'sqlite',
  storage: SQLITE_PATH,
  logging: false,
  dialectModule: sqlite3,
  dialectOptions: { mode: sqlite3.OPEN_READONLY }, // ensure read-only
});

const pg = new Sequelize(PG_URI, {
  logging: false,
  define: { schema: PG_SCHEMA, freezeTableName: true },
});

const qPg = pg.getQueryInterface();

// Tracks per-table metadata such as column type classifications (from PG)
const tableMeta = new Map();

// ---- Helpers ----
function isInternalSqliteTable(name) {
  return name.startsWith('sqlite_');
}

function wantsTable(name) {
  if (INCLUDE_TABLES.length && !INCLUDE_TABLES.includes(name)) return false;
  if (EXCLUDE_TABLES.includes(name)) return false;
  return true;
}

function quoteIdent(name) {
  return '"' + String(name).replace(/"/g, '""') + '"';
}

// Parse `VARCHAR(255)` etc.
function parseLength(typeStr) {
  const m = /\((\d+)\)/i.exec(typeStr);
  return m ? parseInt(m[1], 10) : undefined;
}

// Best-effort SQLite → Sequelize DataTypes mapping for PG target
function mapSqliteTypeToSequelize(typeStrRaw) {
  const typeStr = String(typeStrRaw || '').trim().toUpperCase();
  const len = parseLength(typeStr);

  if (typeStr.includes('INT')) return DataTypes.INTEGER;
  if (/(CHAR|CLOB|TEXT)/.test(typeStr)) return len ? DataTypes.STRING(len) : DataTypes.TEXT;
  if (typeStr.includes('BLOB')) return DataTypes.BLOB;
  if (/(REAL|FLOA|DOUB)/.test(typeStr)) return DataTypes.DOUBLE; // or FLOAT
  if (/(DEC|NUM)/.test(typeStr)) {
	const m = /\((\d+)\s*,\s*(\d+)\)/.exec(typeStr);
	if (m) return DataTypes.DECIMAL(parseInt(m[1], 10), parseInt(m[2], 10));
	return DataTypes.DECIMAL;
  }
  if (typeStr.includes('DATE') || typeStr.includes('TIME')) return DataTypes.DATE;
  if (/^BOOLEAN$/.test(typeStr)) return DataTypes.BOOLEAN;
  if (/^UUID$/.test(typeStr)) return DataTypes.UUID;
  return DataTypes.TEXT; // fallback
}

function normalizeDefaultValue(dflt) {
  if (dflt == null) return undefined;
  const s = String(dflt).trim();
  if (/^CURRENT_TIMESTAMP$/i.test(s)) return literal('CURRENT_TIMESTAMP');
  if (/^CURRENT_DATE$/i.test(s)) return literal('CURRENT_DATE');
  if (/^CURRENT_TIME$/i.test(s)) return literal('CURRENT_TIME');
  if (/^-?\d+(\.\d+)?$/.test(s)) return Number(s);
  const m = s.match(/^'(.*)'$/) || s.match(/^"(.*)"$/);
  if (m) return m[1];
  return literal(s);
}

function isAllDigitsStr(str) {
  if (typeof str !== 'string') return false;
  if (str.length === 0) return false;
  for (let i = 0; i < str.length; i++) {
	const code = str.charCodeAt(i);
	if (code < 48 || code > 57) return false;
  }
  return true;
}

function toJsDate(val) {
  if (val == null || val === '') return null;
  if (val instanceof Date) return val;
  if (typeof val === 'string' && isAllDigitsStr(val)) val = Number(val);
  if (typeof val === 'number') {
	if (val === 0) return null; // treat 0 as "no date"
	if (val >= 1e12) return new Date(val);              // ms epoch
	if (val >= 1e9 && val < 5e12) return new Date(val * 1000); // sec epoch
  }
  const d = new Date(val);
  return isNaN(d.getTime()) ? null : d;
}

function emptyStringToNull(v) {
  if (!TREAT_EMPTY_STRING_AS_NULL) return v;
  if (v == null) return v;
  if (typeof v === 'string' && v.trim() === '') return null;
  return v;
}

function toBoolean(val) {
  val = emptyStringToNull(val);
  if (val == null) return null;
  if (typeof val === 'boolean') return val;
  if (typeof val === 'number') return val !== 0;
  const s = String(val).trim().toLowerCase();
  if (s === 'true' || s === 't' || s === 'yes' || s === 'y' || s === '1') return true;
  if (s === 'false' || s === 'f' || s === 'no' || s === 'n' || s === '0') return false;
  return null;
}

function toIntOrNull(val) {
  val = emptyStringToNull(val);
  if (val == null) return null;
  if (typeof val === 'string') {
	const n = Number(val);
	return Number.isFinite(n) ? Math.trunc(n) : null;
  }
  if (typeof val === 'number') return Number.isFinite(val) ? Math.trunc(val) : null;
  return null;
}

function toFloatOrNull(val) {
  val = emptyStringToNull(val);
  if (val == null) return null;
  if (typeof val === 'string') {
	const n = Number(val);
	return Number.isFinite(n) ? n : null;
  }
  if (typeof val === 'number') return Number.isFinite(val) ? val : null;
  return null;
}

function toDecimalStringOrNull(val) {
  val = emptyStringToNull(val);
  if (val == null) return null;
  if (typeof val === 'string') {
	return /^-?\d+(\.\d+)?([eE][+-]?\d+)?$/.test(val.trim()) ? val.trim() : null;
  }
  if (typeof val === 'number') {
	if (!Number.isFinite(val)) return null;
	return String(val);
  }
  return null;
}

// Classify PG types from QueryInterface.describeTable so we don't depend on Sequelize .key
function classifyPgType(typeStr) {
  const t = String(typeStr || '').toLowerCase();
  if (t.includes('timestamp') || t === 'date' || t.startsWith('time')) return 'date';
  if (t.includes('boolean')) return 'boolean';
  if (t.includes('double') || t.includes('real') || t.includes('float')) return 'float';
  if (t.includes('numeric') || t.includes('decimal')) return 'decimal';
  if (t.includes('int')) return 'int';
  return 'other';
}

// ---- Introspection ----
async function getSqliteTables() {
  const rows = await sqlite.query(
	  "SELECT name, sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name;",
	  { type: QueryTypes.SELECT }
  );
  return rows.map((r) => ({ name: r.name, createSql: r.sql }));
}

async function getSqliteColumns(tableName) {
  return sqlite.query(`PRAGMA table_info(${quoteIdent(tableName)});`, { type: QueryTypes.SELECT });
}

async function getSqliteForeignKeys(tableName) {
  return sqlite.query(`PRAGMA foreign_key_list(${quoteIdent(tableName)});`, { type: QueryTypes.SELECT });
}

async function getSqliteIndexes(tableName) {
  const idxList = await sqlite.query(`PRAGMA index_list(${quoteIdent(tableName)});`, { type: QueryTypes.SELECT });
  const out = [];
  for (const idx of idxList) {
	const details = await sqlite.query(`PRAGMA index_info(${quoteIdent(idx.name)});`, { type: QueryTypes.SELECT });
	out.push({ ...idx, columns: details.map((d) => d.name) });
  }
  return out; // has .unique for unique indexes
}

async function ensurePgSchema(schema) {
  await pg.query(`CREATE SCHEMA IF NOT EXISTS ${quoteIdent(schema)};`);
}

// ---- DDL + type map ----
async function createPgTableFromSqlite(table) {
  const columns = await getSqliteColumns(table.name);
  const attrs = {};

  for (const c of columns) {
	const colName = c.name;
	const type = mapSqliteTypeToSequelize(c.type);
	const allowNull = c.notnull === 0;
	const defaultValue = normalizeDefaultValue(c.dflt_value);

	const colDef = { type, allowNull };

	if (c.pk) {
	  colDef.primaryKey = true;
	  if (/INT/i.test(String(c.type || ''))) colDef.autoIncrement = true;
	}
	if (defaultValue !== undefined) colDef.defaultValue = defaultValue;

	attrs[colName] = colDef;
  }

  const Model = pg.define(table.name, attrs, {
	tableName: table.name,
	freezeTableName: true,
	timestamps: false,
	schema: PG_SCHEMA,
  });

  await Model.sync({ force: true, logging: false });

  // After creation, ask PG what the actual types are and classify
  const desc = await qPg.describeTable({ tableName: table.name, schema: PG_SCHEMA });
  const typesByCol = {};
  for (const [col, info] of Object.entries(desc)) {
	typesByCol[col] = classifyPgType(info.type);
  }
  tableMeta.set(table.name, { typesByCol });

  const indexes = await getSqliteIndexes(table.name);
  for (const idx of indexes) {
	if (idx.unique) {
	  await qPg.addIndex({ tableName: table.name, schema: PG_SCHEMA }, idx.columns, {
		unique: true,
		name: idx.name,
	  });
	}
  }
}

// ---- Copy ----
async function copyTableData(table) {
  const [{ cnt }] = await sqlite.query(`SELECT COUNT(*) as cnt FROM ${quoteIdent(table.name)};`, { type: QueryTypes.SELECT });
  const total = Number(cnt) || 0;
  console.log(`  → ${table.name}: ${total} rows`);
  if (total === 0) return;

  const meta = tableMeta.get(table.name) || { typesByCol: {} };

  let offset = 0;
  while (offset < total) {
	const rows = await sqlite.query(
		`SELECT * FROM ${quoteIdent(table.name)} LIMIT ${BATCH_SIZE} OFFSET ${offset};`,
		{ type: QueryTypes.SELECT }
	);

	const coerced = rows.map((row) => {
	  const out = { ...row };
	  for (const [col, kind] of Object.entries(meta.typesByCol)) {
		const v = out[col];
		if (kind === 'date') out[col] = toJsDate(v);
		else if (kind === 'boolean') out[col] = toBoolean(v);
		else if (kind === 'int') out[col] = toIntOrNull(v);
		else if (kind === 'float') out[col] = toFloatOrNull(v);
		else if (kind === 'decimal') out[col] = toDecimalStringOrNull(v);
	  }
	  return out;
	});

	try {
	  await qPg.bulkInsert({ tableName: table.name, schema: PG_SCHEMA }, coerced, {});
	} catch (e) {
	  console.error(`\nBatch insert failed on table ${table.name} at offset ${offset}. ${DEBUG_ON_ERROR ? 'Falling back to row-by-row...' : ''}`);
	  if (DEBUG_ON_ERROR) {
		for (let i = 0; i < coerced.length; i++) {
		  try {
			await qPg.bulkInsert({ tableName: table.name, schema: PG_SCHEMA }, [coerced[i]], {});
		  } catch (rowErr) {
			console.error(`Row ${offset + i} failed. Values:`);
			const kinds = meta.typesByCol;
			for (const [col, kind] of Object.entries(kinds)) {
			  const v = coerced[i][col];
			  if ((kind === 'int' || kind === 'float' || kind === 'decimal') && (v === '' || v === null)) {
				console.error(`  • ${col} (${kind}) problematic value: ${JSON.stringify(v)}`);
			  }
			}
			throw rowErr;
		  }
		}
	  }
	  throw e;
	}

	offset += rows.length;
	process.stdout.write(`    Inserted ${Math.min(offset, total)} / ${total}\r`);
  }
  process.stdout.write('\n');
}

// ---- FKs ----
async function addForeignKeysForTable(table) {
  const fks = await getSqliteForeignKeys(table.name);
  if (!fks.length) return;

  const byId = new Map();
  for (const fk of fks) {
	if (!byId.has(fk.id)) byId.set(fk.id, []);
	byId.get(fk.id).push(fk);
  }

  for (const [id, parts] of byId.entries()) {
	const fields = parts.map((p) => p.from);
	const references = parts.map((p) => p.to);

	await qPg.addConstraint({ tableName: table.name, schema: PG_SCHEMA }, {
	  fields,
	  type: 'foreign key',
	  name: `${table.name}_fk_${id}`,
	  references: { table: { tableName: parts[0].table, schema: PG_SCHEMA }, fields: references },
	  onUpdate: (parts[0].on_update || 'NO ACTION').replace('RESTRICT', 'NO ACTION'),
	  onDelete: (parts[0].on_delete || 'NO ACTION').replace('RESTRICT', 'NO ACTION'),
	  deferrable: Deferrable.INITIALLY_DEFERRED,
	});
  }
}

// ---- Main ----
async function main() {
  console.log('Connecting…');
  await sqlite.authenticate();
  await pg.authenticate();
  await ensurePgSchema(PG_SCHEMA);

  const tablesAll = await getSqliteTables();
  const tables = tablesAll.filter((t) => !isInternalSqliteTable(t.name) && wantsTable(t.name));

  if (!tables.length) {
	console.log('No tables to migrate. Check INCLUDE_TABLES/EXCLUDE_TABLES.');
	await sqlite.close();
	await pg.close();
	return;
  }

  console.log(`Found ${tables.length} table(s): ${tables.map((t) => t.name).join(', ')}`);

  console.log('\\nCreating tables on PostgreSQL…');
  for (const table of tables) {
	process.stdout.write(`- ${table.name}\\n`);
	await createPgTableFromSqlite(table);
  }

  console.log('\\nCopying data…');
  for (const table of tables) {
	await copyTableData(table);
  }

  if (ADD_FOREIGN_KEYS) {
	console.log('\\nAdding foreign keys (deferred)…');
	for (const table of tables) {
	  await addForeignKeysForTable(table);
	}
  } else {
	console.log('\\nSkipping foreign keys. Set ADD_FOREIGN_KEYS=true to add them after copy.');
  }

  console.log('\\nDone!');
  await sqlite.close();
  await pg.close();
}

// Run
(async () => {
  await main();
})().catch((err) => {
  console.error('\\nError during migration:', err);
  process.exitCode = 1;
});
