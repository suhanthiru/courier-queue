import { DatabaseSync } from 'node:sqlite';
import { randomUUID } from 'node:crypto';

export class Queue {
  constructor(file, now = Date.now) {
    this.db = new DatabaseSync(file);
    this.now = now;
    this.db.exec(
      'PRAGMA busy_timeout = 5000; PRAGMA journal_mode = WAL;' +
      'CREATE TABLE IF NOT EXISTS jobs (' +
      'id TEXT PRIMARY KEY, job_key TEXT UNIQUE NOT NULL, text TEXT NOT NULL,' +
      'state TEXT NOT NULL CHECK (state IN (\'queued\', \'running\', \'succeeded\', \'dead\')),' +
      'attempts INTEGER NOT NULL DEFAULT 0, max_attempts INTEGER NOT NULL,' +
      'available_at INTEGER NOT NULL, lease_until INTEGER, token TEXT,' +
      'error TEXT, result TEXT, created_at INTEGER NOT NULL) STRICT;' +
      'CREATE INDEX IF NOT EXISTS ready ON jobs (state, available_at, created_at, id);' +
      'CREATE INDEX IF NOT EXISTS expired ON jobs (state, lease_until);'
    );
    this.insert = this.db.prepare(
      "INSERT INTO jobs (id, job_key, text, state, max_attempts, available_at, created_at) " +
      "VALUES (?, ?, ?, 'queued', ?, ?, ?) ON CONFLICT(job_key) DO NOTHING"
    );
    this.byKey = this.db.prepare('SELECT * FROM jobs WHERE job_key = ?');
    this.expire = this.db.prepare(
      "UPDATE jobs SET state = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'queued' END, " +
      "available_at = ?, token = NULL, lease_until = NULL, error = 'Worker lease expired' " +
      "WHERE state = 'running' AND lease_until <= ?"
    );
    this.take = this.db.prepare(
      "UPDATE jobs SET state = 'running', attempts = attempts + 1, lease_until = ?, token = ? " +
      "WHERE id = (SELECT id FROM jobs WHERE state = 'queued' AND available_at <= ? " +
      'ORDER BY available_at, created_at, id LIMIT 1) RETURNING *'
    );
    this.complete = this.db.prepare(
      "UPDATE jobs SET state = 'succeeded', result = ?, error = NULL, token = NULL, lease_until = NULL " +
      "WHERE id = ? AND state = 'running' AND token = ? AND lease_until > ?"
    );
    this.retry = this.db.prepare(
      "UPDATE jobs SET state = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'queued' END, " +
      "error = ?, available_at = ? + (1000 << (attempts - 1)), token = NULL, lease_until = NULL " +
      "WHERE id = ? AND state = 'running' AND token = ? AND lease_until > ?"
    );
    this.count = this.db.prepare('SELECT state, COUNT(*) AS count FROM jobs GROUP BY state');
  }

  enqueue(key, text, maxAttempts = 4) {
    if (typeof key !== 'string' || !/^[a-zA-Z0-9_.-]{1,80}$/.test(key)) throw new Error('Invalid job key');
    if (typeof text !== 'string' || Buffer.byteLength(text) > 64 * 1024) throw new Error('Expected a text payload up to 64 KB');
    if (!Number.isInteger(maxAttempts) || maxAttempts < 1 || maxAttempts > 10) throw new Error('maxAttempts must be 1–10');
    const now = this.now();
    const created = this.insert.run(randomUUID(), key, text, maxAttempts, now, now).changes === 1;
    const job = this.byKey.get(key);
    if (job.text !== text || job.max_attempts !== maxAttempts) throw new Error('Job key already belongs to a different request');
    return { id: job.id, created };
  }

  claim(leaseMs = 30_000) {
    if (!Number.isInteger(leaseMs) || leaseMs < 1 || leaseMs > 600_000) throw new Error('Invalid lease duration');
    this.db.exec('BEGIN IMMEDIATE');
    try {
      const now = this.now();
      this.expire.run(now, now);
      const job = this.take.get(now + leaseMs, randomUUID(), now) ?? null;
      this.db.exec('COMMIT');
      return job;
    } catch (error) {
      this.db.exec('ROLLBACK');
      throw error;
    }
  }

  succeed(job, result) {
    const serialized = JSON.stringify(result);
    if (serialized === undefined) throw new Error('Expected a JSON result');
    return this.complete.run(serialized, job.id, job.token, this.now()).changes === 1;
  }

  fail(job, error) {
    const now = this.now();
    return this.retry.run(String(error).slice(0, 500), now, job.id, job.token, now).changes === 1;
  }

  status(key) {
    const job = this.byKey.get(key);
    if (!job) return null;
    return {
      key: job.job_key, state: job.state, attempts: job.attempts,
      result: JSON.parse(job.result), error: job.error,
    };
  }

  metrics() {
    const counts = { queued: 0, running: 0, succeeded: 0, dead: 0 };
    for (const row of this.count.all()) counts[row.state] = row.count;
    return counts;
  }

  close() { this.db.close(); }
}
