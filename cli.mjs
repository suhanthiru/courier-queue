import { createHash } from 'node:crypto';
import { setImmediate as yieldToSignals, setTimeout as delay } from 'node:timers/promises';
import { Queue } from './queue.mjs';

function digest(text) {
  return {
    bytes: Buffer.byteLength(text),
    words: text.trim().split(/\s+/u).filter(Boolean).length,
    sha256: createHash('sha256').update(text).digest('hex'),
  };
}

async function main() {
  const [command, ...args] = process.argv.slice(2);
  const valid = (command === 'enqueue' && args.length === 2) ||
    (command === 'status' && args.length === 1) ||
    (command === 'metrics' && args.length === 0) ||
    (command === 'work' && (args.length === 0 || (args.length === 1 && args[0] === '--once')));
  if (!valid) throw new Error('Usage: node cli.mjs enqueue KEY TEXT | work [--once] | status KEY | metrics');

  const queue = new Queue(process.env.COURIER_DB ?? 'courier.sqlite');
  const stop = new AbortController();
  const shutdown = () => stop.abort();
  process.once('SIGINT', shutdown);
  process.once('SIGTERM', shutdown);
  try {
    if (command === 'enqueue') return console.log(JSON.stringify(queue.enqueue(...args)));
    if (command === 'status') {
      const job = queue.status(args[0]);
      if (!job) throw new Error('Unknown job key');
      return console.log(JSON.stringify(job));
    }
    if (command === 'metrics') return console.log(JSON.stringify(queue.metrics()));

    do {
      const job = queue.claim();
      if (job) {
        await yieldToSignals();
        let result;
        try { result = digest(job.text); }
        catch (error) {
          queue.fail(job, error.message);
          continue;
        }
        const accepted = queue.succeed(job, result);
        console.log(JSON.stringify({ event: accepted ? 'job_succeeded' : 'lease_lost', id: job.id, attempt: job.attempts }));
      } else if (args.length === 0) {
        try { await delay(250, undefined, { signal: stop.signal }); }
        catch (error) { if (error.name !== 'AbortError') throw error; }
      }
    } while (args.length === 0 && !stop.signal.aborted);
  } finally {
    process.off('SIGINT', shutdown);
    process.off('SIGTERM', shutdown);
    queue.close();
  }
}

main().catch((error) => {
  console.error(error.message);
  process.exitCode = 1;
});
