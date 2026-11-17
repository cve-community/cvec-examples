# Node consumer

Tails every CVE change from a cve-streams cluster with [KafkaJS](https://kafka.js.org/).
Needs Node.js 22.18+ (it runs the TypeScript file directly; no build step).

```sh
cd node/consumer
npm install
cp .env.example .env      # then fill in CVEC_BROKERS / CVEC_USERNAME / CVEC_PASSWORD
npm start                 # one JSON line per change, to stdout
```

Useful variations:

```sh
npm start -- --since 2026-11-16          # start from a date (re-reads from there)
npm start -- --out ./cves                # keep ./cves/<year>/<CVE-ID>.json current
npm start -- --topic cve.state           # latest record per CVE instead of every change
npm start | jq -r '"\(.changedAt) \(.op) \(.cveId)"'   # quick human view
npm start -- --help
```

Without `--since`, your consumer group remembers where you stopped: restart and it
picks up from there (a brand-new group starts at the beginning). Groups must start
with `<your username>.` — the default is `<username>.example`.

Each stdout line looks like:

```json
{"cveId":"CVE-2026-129491","partition":2,"offset":"17","changedAt":"2026-11-16T00:15:15.000Z",
 "type":"cve.change","op":"create","commit":"…","committedAt":"…","record":{ …CVE JSON 5.x… }}
```
