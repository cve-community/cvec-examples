// cve-streams example consumer (Node + KafkaJS).
//
// Tails every CVE change from a cve-streams cluster. Each message is one change
// to one CVE record, keyed by CVE ID and timestamped with when it changed
// upstream. Prints one JSON line per change to stdout, or, with --out <dir>,
// keeps <dir>/<year>/<CVE-ID>.json up to date with the latest record.
//
//   npm start                                  # stdout, from where your group left off
//   npm start -- --since 2026-11-16            # (re)start from a date
//   npm start -- --out ~/Downloads/cvec-cves   # mirror records into that folder
//
// Connection settings come from the environment (or a .env file; see
// .env.example). Run with --help for every option.

import { mkdir, rm, writeFile } from "node:fs/promises";
import { homedir } from "node:os";
import { join, resolve } from "node:path";
import { parseArgs } from "node:util";
import { Kafka, logLevel } from "kafkajs";

const { values: args } = parseArgs({
  options: {
    out: { type: "string", short: "o" },
    since: { type: "string", short: "s" },
    topic: { type: "string", short: "t" },
    group: { type: "string", short: "g" },
    quiet: { type: "boolean", short: "q", default: false },
    check: { type: "boolean", default: false },
    help: { type: "boolean", short: "h", default: false },
  },
});

if (args.help) {
  console.log(`Usage: npm start -- [options]

Options (each also settable via env):
  -o, --out <dir>     write each record to <dir>/<year>/<CVE-ID>.json (latest wins;
                      deleted CVEs are removed) instead of printing     CVEC_OUT
  -s, --since <date>  start at this date/time (ISO, e.g. 2026-11-16) instead of
                      where your consumer group left off               CVEC_SINCE
  -t, --topic <name>  cve.changes (every change, default) or cve.state
                      (latest record per CVE)                           CVEC_TOPIC
  -g, --group <id>    consumer group; must start with "<username>."   CVEC_GROUP
                      (default: <username>.example)
  -q, --quiet         with --out: don't log each file written
      --check         connect, authenticate, report what's available, and exit

Connection (required):
  CVEC_BROKERS    host:port the cluster owner gave you, e.g. kafka.example.com:9092
  CVEC_USERNAME   your username, e.g. demo1
  CVEC_PASSWORD   your password
  CVEC_SSL        true if the address is TLS (default false)
  CVEC_MECHANISM  scram-sha-256 (default) or scram-sha-512`);
  process.exit(0);
}

function need(name: string): string {
  const v = process.env[name];
  if (!v) {
    console.error(`Missing ${name}. Copy .env.example to .env and fill it in (or export it). See --help.`);
    process.exit(2);
  }
  return v;
}

const brokers = need("CVEC_BROKERS").split(",").map((b) => b.trim());
const username = need("CVEC_USERNAME");
const password = need("CVEC_PASSWORD");
const ssl = (process.env.CVEC_SSL ?? "false") === "true";
const mechanism = process.env.CVEC_MECHANISM === "scram-sha-512" ? "scram-sha-512" : "scram-sha-256";
const topic = args.topic ?? process.env.CVEC_TOPIC ?? "cve.changes";
const groupId = args.group ?? process.env.CVEC_GROUP ?? `${username}.example`;
// Relative --out paths are from the directory you ran the command in: npm
// changes into this package's folder first but records the original in INIT_CWD.
const outRaw = args.out ?? process.env.CVEC_OUT;
const outDir = outRaw
  ? resolve(process.env.INIT_CWD ?? process.cwd(), outRaw.replace(/^~(?=\/|$)/, homedir()))
  : undefined;
const sinceRaw = args.since ?? process.env.CVEC_SINCE;
const since = sinceRaw ? new Date(sinceRaw) : undefined;
if (since && Number.isNaN(since.getTime())) {
  console.error(`--since must be an ISO date/time, got "${sinceRaw}"`);
  process.exit(2);
}
if (!groupId.startsWith(`${username}.`)) {
  console.error(`Consumer group must start with "${username}." (your account can only use those).`);
  process.exit(2);
}

// stdout carries the data; everything else goes to stderr.
const info = (msg: string) => console.error(`[cvec] ${msg}`);

const kafka = new Kafka({
  clientId: `cvec-example-${username}`,
  brokers,
  ssl,
  sasl: mechanism === "scram-sha-512"
    ? { mechanism: "scram-sha-512", username, password }
    : { mechanism: "scram-sha-256", username, password },
  logLevel: logLevel.ERROR,
});

// Values are JSON:
//   cve.changes: { type: "cve.change", cveId, op: create|update|delete, commit, committedAt, record }
//   cve.state:   { type: "cve.state", cveId, commit, committedAt, record }   (null = deleted)
type Change = { cveId?: string; op?: string; committedAt?: string; record?: unknown };

async function writeRecord(dir: string, cveId: string, value: Change | null): Promise<string> {
  const year = cveId.split("-")[1] ?? "unknown";
  const path = join(dir, year, `${cveId}.json`);
  if (!value || value.op === "delete" || value.record == null) {
    await rm(path, { force: true });
    return `removed ${path}`;
  }
  await mkdir(join(dir, year), { recursive: true });
  await writeFile(path, JSON.stringify(value.record, null, 2) + "\n");
  return `wrote ${path}`;
}

// --check: prove the address and credentials work without consuming anything.
async function check(): Promise<void> {
  const admin = kafka.admin();
  await admin.connect();
  const offsets = await admin.fetchTopicOffsets(topic);
  const total = offsets.reduce((n, p) => n + Number(p.high) - Number(p.low), 0);
  await admin.disconnect();
  console.log(`OK: authenticated as ${username} at ${brokers.join(",")}; ${topic} has ${total} messages across ${offsets.length} partitions.`);
}

async function main(): Promise<void> {
  if (args.check) {
    await check();
    process.exit(0);
  }
  // Starting offsets: --since wins; otherwise resume the group (a new group
  // starts from the beginning of the topic).
  let seekTo: { partition: number; offset: string }[] | undefined;
  if (since) {
    const admin = kafka.admin();
    await admin.connect();
    seekTo = await admin.fetchTopicOffsetsByTimestamp(topic, since.getTime());
    await admin.disconnect();
  }

  const consumer = kafka.consumer({ groupId });
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
  info(`connected to ${brokers.join(",")} as ${username}; topic ${topic}, group ${groupId}` +
    (since ? `, from ${since.toISOString()}` : "") + (outDir ? `, writing to ${outDir}` : ""));

  let count = 0;
  await consumer.run({
    eachMessage: async ({ partition, message }) => {
      const key = message.key?.toString() ?? "";
      const value = message.value ? (JSON.parse(message.value.toString()) as Change) : null;
      count++;
      if (outDir) {
        const what = await writeRecord(outDir, key, value);
        if (!args.quiet) info(what);
      } else {
        process.stdout.write(JSON.stringify({
          cveId: key,
          partition,
          offset: message.offset,
          changedAt: new Date(Number(message.timestamp)).toISOString(),
          ...(value ?? { op: "delete", record: null }),
        }) + "\n");
      }
    },
  });

  if (seekTo) for (const { partition, offset } of seekTo) consumer.seek({ topic, partition, offset });

  const stop = async () => {
    info(`stopping after ${count} messages`);
    await consumer.disconnect();
    process.exit(0);
  };
  process.on("SIGINT", stop);
  process.on("SIGTERM", stop);
}

main().catch((err) => {
  console.error(`[cvec] ${err?.message ?? err}`);
  if (/SASL|authentication/i.test(String(err))) console.error("[cvec] check CVEC_USERNAME / CVEC_PASSWORD");
  if (/ECONNREFUSED|ENOTFOUND|timed out|Connection/i.test(String(err))) console.error("[cvec] check CVEC_BROKERS (and that the cluster/tunnel is up)");
  process.exit(1);
});
