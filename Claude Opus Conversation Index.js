import express from 'express';
import pg from 'pg';
import cors from 'cors';

const { Pool } = pg;
const app = express();
const PORT = process.env.PORT || 3001;

// Postgres connection
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
});

// Middleware
app.use(cors());
app.use(express.json({ limit: '50mb' }));

// Initialize database tables
async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS records (
      id SERIAL PRIMARY KEY,
      data JSONB NOT NULL,
      employer TEXT GENERATED ALWAYS AS (data->>'_e') STORED,
      record_type TEXT GENERATED ALWAYS AS (data->>'_t') STORED,
      state TEXT GENERATED ALWAYS AS (data->>'_s') STORED,
      created_at TIMESTAMP DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_records_employer ON records(employer);
    CREATE INDEX IF NOT EXISTS idx_records_type ON records(record_type);
    CREATE INDEX IF NOT EXISTS idx_records_state ON records(state);

    CREATE TABLE IF NOT EXISTS ladders (
      id TEXT PRIMARY KEY,
      data JSONB NOT NULL,
      employer TEXT,
      created_at TIMESTAMP DEFAULT NOW()
    );
  `);
  console.log('✅ Database tables initialized');
}

// ── RECORDS API ──

// GET /api/records — get all records
app.get('/api/records', async (req, res) => {
  try {
    const { rows } = await pool.query('SELECT data FROM records ORDER BY id');
    res.json(rows.map(r => r.data));
  } catch (err) {
    console.error('GET /api/records error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/records — save new records (append)
app.post('/api/records', async (req, res) => {
  try {
    const records = req.body;
    if (!Array.isArray(records) || records.length === 0) {
      return res.status(400).json({ error: 'Expected array of records' });
    }

    // Insert in batches of 500
    const BATCH = 500;
    let inserted = 0;
    for (let i = 0; i < records.length; i += BATCH) {
      const batch = records.slice(i, i + BATCH);
      const values = batch.map((r, idx) => `($${idx + 1}::jsonb)`).join(',');
      const params = batch.map(r => JSON.stringify(r));
      await pool.query(`INSERT INTO records (data) VALUES ${values}`, params);
      inserted += batch.length;
    }

    const { rows: [{ count }] } = await pool.query('SELECT COUNT(*)::int as count FROM records');
    res.json({ inserted, total: count });
  } catch (err) {
    console.error('POST /api/records error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// PUT /api/records — replace ALL records (used by save/overwrite)
app.put('/api/records', async (req, res) => {
  try {
    const records = req.body;
    if (!Array.isArray(records)) {
      return res.status(400).json({ error: 'Expected array of records' });
    }

    await pool.query('BEGIN');
    await pool.query('DELETE FROM records');

    const BATCH = 500;
    for (let i = 0; i < records.length; i += BATCH) {
      const batch = records.slice(i, i + BATCH);
      const values = batch.map((r, idx) => `($${idx + 1}::jsonb)`).join(',');
      const params = batch.map(r => JSON.stringify(r));
      await pool.query(`INSERT INTO records (data) VALUES ${values}`, params);
    }

    await pool.query('COMMIT');
    res.json({ total: records.length });
  } catch (err) {
    await pool.query('ROLLBACK');
    console.error('PUT /api/records error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// DELETE /api/records/employer/:name — delete all records for an employer
app.delete('/api/records/employer/:name', async (req, res) => {
  try {
    const { rowCount } = await pool.query(
      "DELETE FROM records WHERE data->>'_e' = $1",
      [req.params.name]
    );
    res.json({ deleted: rowCount });
  } catch (err) {
    console.error('DELETE employer error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// DELETE /api/records/role — delete records for a specific role at an employer
app.delete('/api/records/role', async (req, res) => {
  try {
    const { employer, role } = req.query;
    const { rowCount } = await pool.query(
      `DELETE FROM records WHERE data->>'_e' = $1 AND (
        data->>'role' = $2 OR data->>'cls' = $2 OR data->>'role_title' = $2
      )`,
      [employer, role]
    );
    res.json({ deleted: rowCount });
  } catch (err) {
    console.error('DELETE role error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// DELETE /api/records — delete all records (reset)
app.delete('/api/records', async (req, res) => {
  try {
    await pool.query('DELETE FROM records');
    await pool.query('DELETE FROM ladders');
    res.json({ ok: true });
  } catch (err) {
    console.error('DELETE all error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── LADDERS API ──

// GET /api/ladders
app.get('/api/ladders', async (req, res) => {
  try {
    const { rows } = await pool.query('SELECT data FROM ladders ORDER BY created_at');
    res.json(rows.map(r => r.data));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/ladders
app.post('/api/ladders', async (req, res) => {
  try {
    const ladder = req.body;
    await pool.query(
      'INSERT INTO ladders (id, data, employer) VALUES ($1, $2::jsonb, $3) ON CONFLICT (id) DO UPDATE SET data = $2::jsonb',
      [ladder.id, JSON.stringify(ladder), ladder.emp]
    );
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// DELETE /api/ladders/:id
app.delete('/api/ladders/:id', async (req, res) => {
  try {
    await pool.query('DELETE FROM ladders WHERE id = $1', [req.params.id]);
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// DELETE /api/ladders/employer/:name
app.delete('/api/ladders/employer/:name', async (req, res) => {
  try {
    await pool.query('DELETE FROM ladders WHERE employer = $1', [req.params.name]);
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Health check
app.get('/api/health', async (req, res) => {
  try {
    const { rows: [{ count }] } = await pool.query('SELECT COUNT(*)::int as count FROM records');
    res.json({ status: 'ok', records: count });
  } catch (err) {
    res.json({ status: 'error', error: err.message });
  }
});

// Start
initDB().then(() => {
  app.listen(PORT, '0.0.0.0', () => {
    console.log(`🚀 WagePage API running on port ${PORT}`);
  });
}).catch(err => {
  console.error('Failed to init DB:', err);
  process.exit(1);
});
