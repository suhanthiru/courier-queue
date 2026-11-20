import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { Queue } from './queue.mjs';

test('persists jobs, fences an expired lease and allows a second worker to finish', () => {
  const folder = mkdtempSync(join(tmpdir(), 'courier-'));
  const file = join(folder, 'queue.sqlite');
  let time = 100;
  const first = new Queue(file, () => time);
  const second = new Queue(file, () => time);
  try {
    const receipt = first.enqueue('metrics-1', 'two words');
    assert.equal(second.enqueue('metrics-1', 'two words').id, receipt.id);
    assert.throws(() => second.enqueue('metrics-1', 'another payload'), /different request/);
    const oldLease = first.claim(10);
    assert.equal(second.claim(10), null);
    time += 10;
    assert.equal(first.succeed(oldLease, { old: true }), false);
    const newLease = second.claim(10);
    assert.equal(newLease.id, oldLease.id);
    assert.equal(newLease.attempts, 2);
    assert.equal(first.fail(oldLease, 'stale worker'), false);
    assert.equal(second.succeed(newLease, { words: 2 }), true);
    assert.deepEqual(first.status('metrics-1'), { key: 'metrics-1', state: 'succeeded', attempts: 2, result: { words: 2 }, error: null });
  } finally {
    first.close();
    second.close();
    rmSync(folder, { recursive: true, force: true });
  }
});

test('applies retry delay and dead-letters after failed or expired final attempts', () => {
  let time = 10;
  const queue = new Queue(':memory:', () => time);
  try {
    queue.enqueue('retry', 'body', 2);
    assert.equal(queue.fail(queue.claim(), 'temporary failure'), true);
    time += 999;
    assert.equal(queue.claim(), null);
    time += 1;
    assert.equal(queue.fail(queue.claim(), 'last failure'), true);
    queue.enqueue('expire', 'body', 1);
    assert.equal(queue.claim(5).job_key, 'expire');
    time += 5;
    assert.equal(queue.claim(), null);
    assert.deepEqual(queue.metrics(), { queued: 0, running: 0, succeeded: 0, dead: 2 });
    assert.equal(queue.status('expire').error, 'Worker lease expired');
  } finally { queue.close(); }
});
