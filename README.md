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
then either signs you up with an **invite code** (you pick a username and give
an email) or takes the address/username/password you were given, and tests
the connection. It asks whether you want the feed printed or written to one
file per CVE, saves your settings and starts consuming. Re-run it any time; it
remembers your answers.

Signing up gives you an account login (for the registrar API: keys, invites,
consumer groups, password resets), a key for this machine, and invite codes of
your own to pass on; the quickstart saves them to `~/cvec-examples/account.txt`.

## What you're consuming

| Topic | Contents |
|---|---|
| `cve.changes` | One message per change to a CVE record, in order. Key = CVE ID; timestamp = when it changed upstream; value = JSON with the full [CVE JSON 5.x](https://github.com/CVEProject/cve-schema) record. |
| `cve.state` | The latest record per CVE (compacted). A null value means the CVE was deleted. |

Records come from the official [CVE List](https://github.com/CVEProject/cvelistV5).
Access is read-only: a key can read `cve.*` topics and use consumer groups in its
namespace. Keys are named `<namespace>.<key>` (e.g. `alice.default`, `acme.ci`),
and their groups start with `<namespace>.` (`alice.*`, `acme.*`), so every key
of an organization shares its groups and their positions.

## Connecting

You need a broker address (`host:port`), a username and a password: from
signing up (the quickstart does it), or from whoever runs the cluster.
Authentication is SASL/SCRAM-SHA-256. Any Kafka client works; the examples
here just make it quick.

Give each machine or job its own key (create, rotate and revoke them through
the registrar API, with your account login), so one can be replaced without
touching the others.
