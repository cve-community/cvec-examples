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
//   npm start -- groups                        # your consumer groups and their lag
//   npm start -- groups reset <group> --to 2026-11-01   # move a group (stop it first)
//
// Connection settings come from the environment (or a .env file; see
// .env.example). Run with --help for every option.

import { mkdir, rm, writeFile } from "node:fs/promises";
import { homedir } from "node:os";
import { join, resolve } from "node:path";
import { parseArgs } from "node:util";
import { Kafka, logLevel } from "kafkajs";

const { values: args, positionals } = parseArgs({
  allowPositionals: true,
  options: {
    out: { type: "string", short: "o" },
    since: { type: "string", short: "s" },
    topic: { type: "string", short: "t" },
    group: { type: "string", short: "g" },
    quiet: { type: "boolean", short: "q", default: false },
    check: { type: "boolean", default: false },
    help: { type: "boolean", short: "h", default: false },
    to: { type: "string" },
  },
});

if (args.help) {
  console.log(`Usage: npm start -- [options]
       npm start -- groups [list]                       your consumer groups: state, members, lag
       npm start -- groups reset <group> --to <where>   beginning | end | a date (e.g. 2026-11-01);
                                                        the group must have no running consumer
       npm start -- groups delete <group>               forget a group (next use starts over)

Options (each also settable via env):
  -o, --out <dir>     write each record to <dir>/<year>/<CVE-ID>.json (latest wins;
                      deleted CVEs are removed) instead of printing     CVEC_OUT
  -s, --since <date>  start at this date/time (ISO, e.g. 2026-11-16) instead of
                      where your consumer group left off               CVEC_SINCE
  -t, --topic <name>  cve.changes (every change, default) or cve.state
                      (latest record per CVE)                           CVEC_TOPIC
  -g, --group <id>    consumer group; must start with your namespace  CVEC_GROUP
                      ("acme." for key acme.ci; default: <username>.example)
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
// Consumer groups belong to your namespace: for a key "acme.ci" that's
// "acme.", for a plain username like "demo1" it's "demo1.".
const namespace = username.includes(".") ? username.slice(0, username.indexOf(".")) : username;
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
if (!groupId.startsWith(`${namespace}.`)) {
  console.error(`Consumer group must start with "${namespace}." (your account can only use those).`);
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

// --- groups: list / reset / delete, straight over Kafka with your own key ---
// Your key may read, describe and delete groups in your namespace, which is
// all these need. (Standard tools work too: kcat, rpk, kafka-consumer-groups.)
async function groups(): Promise<void> {
  const [, cmd = "list", group] = positionals;
  const admin = kafka.admin();
  await admin.connect();
  try {
    if (cmd === "list") {
      const ids = (await admin.listGroups()).groups.map((g) => g.groupId).filter((g) => g.startsWith(`${namespace}.`)).sort();
      if (!ids.length) { console.log(`No consumer groups in "${namespace}." yet.`); return; }
      const { groups: described } = await admin.describeGroups(ids);
      for (const d of described) {
        let lag = 0;
        for (const t of await admin.fetchOffsets({ groupId: d.groupId })) {
          const ends = new Map((await admin.fetchTopicOffsets(t.topic)).map((o) => [o.partition, Number(o.high)]));
          for (const p of t.partitions) if (Number(p.offset) >= 0) lag += Math.max(0, (ends.get(p.partition) ?? 0) - Number(p.offset));
        }
        console.log(`${d.groupId.padEnd(40)} ${d.state.padEnd(12)} members=${d.members.length}  lag=${lag}`);
      }
      return;
    }
    if (!group) throw new Error(`usage: npm start -- groups ${cmd} <group>`);
    if (!group.startsWith(`${namespace}.`)) throw new Error(`"${group}" isn't yours; your groups start with "${namespace}."`);
    const [d] = (await admin.describeGroups([group])).groups;
    if (d && d.members.length && d.state !== "Empty") throw new Error(`${group} has ${d.members.length} running consumer(s); stop them first`);
    if (cmd === "delete") {
      await admin.deleteGroups([group]);
      console.log(`Deleted ${group}.`);
      return;
    }
    if (cmd !== "reset") throw new Error(`unknown groups command "${cmd}" (list, reset, delete)`);
    const to = args.to;
    if (!to) throw new Error("reset needs --to beginning | end | <date>");
    const ts = to === "beginning" || to === "end" ? undefined : Date.parse(to);
    if (ts !== undefined && Number.isNaN(ts)) throw new Error(`--to must be beginning, end or a date, got "${to}"`);
    const ends = await admin.fetchTopicOffsets(topic);
    const offsets = ts === undefined
      ? ends.map((o) => ({ partition: o.partition, offset: to === "beginning" ? o.low : o.high }))
      : (await admin.fetchTopicOffsetsByTimestamp(topic, ts)).map((o) => ({
          partition: o.partition, offset: Number(o.offset) < 0 ? ends.find((e) => e.partition === o.partition)!.high : o.offset }));
    // Commit as a brief member of the group (KafkaJS's admin.setOffsets can hang).
    const c = kafka.consumer({ groupId: group, sessionTimeout: 10000, heartbeatInterval: 1000 });
    await c.connect();
    await c.subscribe({ topic, fromBeginning: true });
    const joined = new Promise<void>((resolve) => { c.on(c.events.GROUP_JOIN, () => resolve()); });
    await c.run({ autoCommit: false, eachBatchAutoResolve: false, eachBatch: async () => {} });
    await joined;
    c.pause([{ topic }]);
    await c.commitOffsets(offsets.map((o) => ({ topic, ...o })));
    await c.disconnect();
    console.log(`${group} on ${topic} now at: ${offsets.map((o) => `p${o.partition}=${o.offset}`).join(" ")}`);
  } finally {
    await admin.disconnect();
  }
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
  if (positionals[0] === "groups") {
    await groups();
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
