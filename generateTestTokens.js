#!/usr/bin/env node
/**
 * Test Token Generator
 * Generates fake FCM-style device tokens for load testing.
 *
 * Usage:
 *   node generateTestTokens.js --count 20000000 --output tokens.txt
 */

import fs      from 'fs';
import crypto  from 'crypto';
import yargs   from 'yargs';
import { hideBin } from 'yargs/helpers';

const argv = yargs(hideBin(process.argv))
  .option('count',  { type: 'number', default: 1_000_000, describe: 'Number of tokens to generate' })
  .option('output', { type: 'string', default: 'tokens.txt', describe: 'Output file path' })
  .help()
  .parseSync();

/**
 * Generate one fake FCM-like token (152 chars, similar to real tokens).
 * Real FCM tokens are base64url-encoded strings of varying length.
 */
function fakeFcmToken() {
  return [
    crypto.randomBytes(32).toString('base64url'),
    crypto.randomBytes(32).toString('base64url'),
    crypto.randomBytes(16).toString('base64url'),
  ].join(':');
}

async function main() {
  const { count, output } = argv;
  console.log(`Generating ${count.toLocaleString()} test tokens → ${output}`);

  const CHUNK = 50_000; // write in chunks to avoid huge memory usage
  const stream = fs.createWriteStream(output, { flags: 'w', highWaterMark: 1 << 20 });

  let written = 0;

  while (written < count) {
    const batchSize = Math.min(CHUNK, count - written);
    const lines = [];

    for (let i = 0; i < batchSize; i++) {
      lines.push(fakeFcmToken());
    }

    // Respect backpressure from the write stream
    const canContinue = stream.write(lines.join('\n') + '\n');
    if (!canContinue) {
      await new Promise((resolve) => stream.once('drain', resolve));
    }

    written += batchSize;
    process.stdout.write(`\r  ${written.toLocaleString().padStart(14)} / ${count.toLocaleString()} tokens written`);
  }

  await new Promise((resolve, reject) => {
    stream.end((err) => (err ? reject(err) : resolve()));
  });

  console.log(`\nDone. ${written.toLocaleString()} tokens written to ${output}`);
}

main().catch((err) => {
  console.error(err.message);
  process.exit(1);
});
