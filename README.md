# cvec-examples

Example clients for **cve-streams**: a Kafka-native feed of every CVE change.

| Language | Example |
|---|---|
| Node.js | [`node/consumer`](node/consumer) — tail changes to stdout or mirror records into a directory |

More languages to come.

## Quickstart

```sh
curl -fsSL https://raw.githubusercontent.com/cve-community/cvec-examples/main/quickstart.sh | bash
```

It checks for git and Node.js 22.18+, downloads this repo to `~/cvec-examples`,
asks for the address/username/password you were given (and tests them), asks
whether you want the feed printed or written to one file per CVE, saves your
settings and starts consuming. Re-run it any time; it remembers your answers.

## What you're consuming

| Topic | Contents |
|---|---|
| `cve.changes` | One message per change to a CVE record, in order. Key = CVE ID; timestamp = when it changed upstream; value = JSON with the full [CVE JSON 5.x](https://github.com/CVEProject/cve-schema) record. |
| `cve.state` | The latest record per CVE (compacted). A null value means the CVE was deleted. |

Records come from the official [CVE List](https://github.com/CVEProject/cvelistV5).
Your account is read-only: it can read `cve.*` topics and use consumer groups named
`<your username>.*`.

## Connecting

You need three things from whoever runs the cluster: a broker address
(`host:port`), a username and a password. Authentication is SASL/SCRAM-SHA-256.
Any Kafka client works; the examples here just make it quick.
