#!/usr/bin/env bash
# cve-streams quickstart: walks you through consuming the CVE change feed with
# the Node example in this repo. Safe to re-run; it reuses what's already done.
#
#   curl -fsSL <this script's raw URL> | bash
#   bash quickstart.sh
#
# Every question can be pre-answered with an environment variable (handy for a
# shared link that already knows the address, or for scripted runs):
#   CVEC_REGISTRAR registrar URL, for signing up with an invite code
#   CVEC_INVITE    invite code (sign up instead of entering credentials)
#   CVEC_EMAIL     your email (sign-up; used for password resets)
#   CVEC_BROKERS   broker address, host:port
#   CVEC_USERNAME  your username (sign-up: the one you want)
#   CVEC_PASSWORD  your password (prefer typing it when asked)
#   CVEC_MODE      stdout | files
#   CVEC_OUT       directory for files mode, relative to where you run this
#                  (default: ~/Downloads/cvec-cves)
#   CVEC_SINCE     start date, ISO; "beginning" for all  (default: beginning)
#   CVEC_TOPIC     cve.changes | cve.state             (default: cve.changes)
#   CVEC_DIR       where to put the examples repo      (default: ~/cvec-examples)
#   CVEC_REPO      repo to clone  (default: https://github.com/cve-community/cvec-examples.git)
#   CVEC_START     yes | no: start consuming at the end (default: ask)
#   CVEC_NONINTERACTIVE=1  never prompt; fail if something required is missing
set -euo pipefail

REPO="${CVEC_REPO:-https://github.com/cve-community/cvec-examples.git}"
DIR="${CVEC_DIR:-$HOME/cvec-examples}"
MIN_NODE="22.18.0"
# Relative paths you type (e.g. the output directory) mean "from where you ran
# this", even though the script later works inside the examples repo.
CALLER_DIR=$PWD

# --- terminal helpers -----------------------------------------------------------
# Under `curl | bash`, stdin is the script itself, so prompts read the terminal.
if [ -t 1 ]; then B=$'\033[1m'; D=$'\033[2m'; G=$'\033[32m'; Y=$'\033[33m'; R=$'\033[31m'; N=$'\033[0m'; else B= D= G= Y= R= N=; fi
step()  { printf '\n%s==> %s%s\n' "$B" "$*" "$N"; }
say()   { printf '    %s\n' "$*"; }
note()  { printf '    %s%s%s\n' "$D" "$*" "$N"; }
ok()    { printf '    %s✓%s %s\n' "$G" "$N" "$*"; }
warn()  { printf '    %s!%s %s\n' "$Y" "$N" "$*"; }
die()   { printf '\n%sxx %s%s\n' "$R" "$*" "$N" >&2; exit 1; }

interactive() { [ "${CVEC_NONINTERACTIVE:-0}" != 1 ] && [ -r /dev/tty ]; }

# ask VAR "Question" [default] [secret]  — skips if VAR is already set
ask() {
  local var=$1 q=$2 def=${3:-} secret=${4:-} ans=""
  if [ -n "${!var:-}" ]; then
    printf -v "$var" '%s' "$(clean "${!var}")"
    if [ -n "$secret" ]; then ok "$q (provided)"; else ok "$q ${!var}"; fi
    return
  fi
  interactive || { [ -n "$def" ] && { printf -v "$var" '%s' "$def"; return; }; die "$var is required (non-interactive run)"; }
  while [ -z "$ans" ]; do
    if [ -n "$secret" ]; then
      printf '    %s ' "$q" >/dev/tty; IFS= read -r -s ans </dev/tty; printf '\n' >/dev/tty
    else
      printf '    %s%s ' "$q" "${def:+ [$def]}" >/dev/tty; IFS= read -r ans </dev/tty
      ans=${ans:-$def}
    fi
    [ -n "$ans" ] || warn "this one's required"
  done
  printf -v "$var" '%s' "$(clean "$ans")"
}

# Pasted answers can carry terminal paste markers (ESC[200~ … ESC[201~), a
# carriage return or stray spaces; none of them belong in an address,
# username or password.
clean() {
  local v=$1 esc=$'\033'
  v=${v//"${esc}[200~"/}; v=${v//"${esc}[201~"/}; v=${v//$'\r'/}
  v=${v#"${v%%[![:space:]]*}"}; v=${v%"${v##*[![:space:]]}"}
  printf '%s' "$v"
}

# choose VAR "Question" default opt1 opt2 ...
choose() {
  local var=$1 q=$2 def=$3; shift 3
  if [ -n "${!var:-}" ]; then ok "$q ${!var}"; return; fi
  if ! interactive; then printf -v "$var" '%s' "$def"; return; fi
  local ans opts="$*"
  while :; do
    printf '    %s (%s) [%s] ' "$q" "${opts// //}" "$def" >/dev/tty; IFS= read -r ans </dev/tty
    ans=${ans:-$def}
    case " $opts " in *" $ans "*) printf -v "$var" '%s' "$ans"; return ;; esac
    warn "pick one of: $opts"
  done
}

version_ge() {  # version_ge 22.18.0 22.9.1 -> is $1 >= $2
  [ "$(printf '%s\n%s\n' "$2" "$1" | sort -t. -k1,1n -k2,2n -k3,3n | head -1)" = "$2" ]
}

# --- 0. hello --------------------------------------------------------------------
cat <<EOF

${B}cve-streams quickstart${N}

cve-streams is a Kafka feed of every change to the official CVE List: each
message is one CVE record that was created, updated or deleted, in order, with
the full CVE JSON. This walks you through reading it with a small Node script:

  1. check you have git and Node.js ${MIN_NODE}+
  2. download the example (${REPO})
  3. sign up with an invite code, or use credentials you were given; test them
  4. ask how you want the data (printed, or one file per CVE)
  5. save your settings and start it

Nothing is installed system-wide; it all lives in ${DIR}.
EOF

# --- 1. prerequisites -------------------------------------------------------------
step "1/5  Checking prerequisites"
command -v git >/dev/null || die "git isn't installed. Install it (macOS: xcode-select --install) and re-run."
ok "git $(git --version | awk '{print $3}')"
command -v node >/dev/null || die "Node.js isn't installed. Get ${MIN_NODE}+ from https://nodejs.org (or: brew install node) and re-run."
command -v npm >/dev/null || die "npm isn't installed (it ships with Node.js)."
NODE_V=$(node -p 'process.versions.node')
version_ge "$NODE_V" "$MIN_NODE" || die "Node.js $NODE_V is too old: the example runs TypeScript directly, which needs ${MIN_NODE}+. Upgrade from https://nodejs.org and re-run."
ok "node $NODE_V, npm $(npm -v)"

# --- 2. download -----------------------------------------------------------------
step "2/5  Downloading the example"
note "A public repo of example clients; the Node one is node/consumer/node-consumer.ts."
if [ -d "$DIR/.git" ]; then
  git -C "$DIR" pull -q --ff-only 2>/dev/null && ok "updated $DIR" || warn "couldn't update $DIR (local changes?); using it as is"
else
  git clone -q "$REPO" "$DIR"
  ok "cloned into $DIR"
fi
APP="$DIR/node/consumer"
[ -f "$APP/node-consumer.ts" ] || die "$APP/node-consumer.ts not found; is CVEC_REPO right?"
note "Installing its one dependency (kafkajs) locally..."
(cd "$APP" && npm install --silent --no-audit --no-fund >/dev/null)
ok "dependencies installed in $APP/node_modules"

# Reuse earlier answers on a re-run (never overrides what you've set now).
if [ -f "$APP/.env" ]; then
  while IFS='=' read -r k v; do
    case "$k" in CVEC_BROKERS|CVEC_USERNAME|CVEC_PASSWORD|CVEC_SSL) [ -n "${!k:-}" ] || printf -v "$k" '%s' "$v" ;; esac
  done < <(grep -E '^CVEC_[A-Z_]+=' "$APP/.env")
  note "found your settings from last time in $APP/.env"
fi

# --- 3. connect ------------------------------------------------------------------
step "3/5  Connecting"
if [ -z "${CVEC_INVITE:-}" ] && [ -z "${CVEC_USERNAME:-}" ]; then
  say "You need either an invite code (to create your own account) or a username and"
  say "password someone gave you."
  choose CVEC_HAVE "Do you have an invite code or credentials?" invite invite credentials
else
  CVEC_HAVE=$([ -n "${CVEC_INVITE:-}" ] && echo invite || echo credentials)
fi

if [ "$CVEC_HAVE" = invite ]; then
  note "Signing up creates your account plus a key for this machine. Your email is only"
  note "used to confirm it's you and for password / key resets."
  ask CVEC_REGISTRAR "Registrar URL (whoever invited you has it):"
  CVEC_REGISTRAR=${CVEC_REGISTRAR%/}
  ask CVEC_INVITE "Invite code:"
  while :; do
    ask CVEC_USERNAME "Pick a username (3-31: lowercase letters, digits, dashes):"
    ask CVEC_EMAIL "Your email:"
    body=$(CVEC_INVITE="$CVEC_INVITE" CVEC_USERNAME="$CVEC_USERNAME" CVEC_EMAIL="$CVEC_EMAIL" node -e \
      'console.log(JSON.stringify({invite: process.env.CVEC_INVITE, username: process.env.CVEC_USERNAME, email: process.env.CVEC_EMAIL}))')
    resp=$(curl -sS -w '\n%{http_code}' -X POST "$CVEC_REGISTRAR/v1/register" -H 'content-type: application/json' -d "$body") \
      || die "Couldn't reach $CVEC_REGISTRAR. Check the URL with whoever invited you."
    code=${resp##*$'\n'}; resp=${resp%$'\n'*}
    field() { printf '%s' "$resp" | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{const v=process.argv[1].split(".").reduce((o,k)=>o?.[k],JSON.parse(s));console.log(Array.isArray(v)?v.join(" "):v??"")})' "$1"; }
    [ "$code" = 201 ] && break
    msg=$(field error.message); errc=$(field error.code)
    case "$errc" in
      name_taken|name_reserved|bad_name) warn "$msg"; interactive || die "$msg"; CVEC_USERNAME= ;;
      email_taken|bad_email) warn "$msg"; interactive || die "$msg"; CVEC_EMAIL= ;;
      *) die "Sign-up failed: ${msg:-HTTP $code}" ;;
    esac
  done
  CVEC_BROKERS=$(field kafka.brokers); CVEC_USERNAME=$(field kafka.username); CVEC_PASSWORD=$(field kafka.password)
  ACCOUNT_USER=$(field account.username); ACCOUNT_PW=$(field account.password); INVITES=$(field invites)
  ok "account $ACCOUNT_USER created; this machine's key is $CVEC_USERNAME"
  umask 077
  mkdir -p "$DIR"
  {
    echo "# cve-streams account (written by quickstart.sh; keep it private)"
    echo "registrar=$CVEC_REGISTRAR"
    echo "username=$ACCOUNT_USER"
    echo "password=$ACCOUNT_PW"
    echo "# Invite codes you can give to others (one person each):"
    for c in $INVITES; do echo "invite=$c"; done
  } > "$DIR/account.txt"
  ok "account login + your invite codes saved to $DIR/account.txt"
  note "Check your inbox for a confirmation link: confirming lets you reset passwords later."
else
  note "Whoever runs the cluster gave you an address (host:port), a username and a password."
  ask CVEC_BROKERS "Broker address (host:port):"
  ask CVEC_USERNAME "Username:"
  ask CVEC_PASSWORD "Password (typing is hidden):" "" secret
fi
CVEC_SSL="${CVEC_SSL:-false}"
# The script passes the start date itself (first run only); never via env.
export -n CVEC_SINCE 2>/dev/null || true
export CVEC_BROKERS CVEC_USERNAME CVEC_PASSWORD CVEC_SSL
note "Testing the connection..."
if out=$(cd "$APP" && node --disable-warning=TimeoutNegativeWarning node-consumer.ts --check 2>&1); then
  ok "${out#OK: }"
else
  printf '%s\n' "$out" | tail -5 | sed 's/^/    /' >&2
  note "(you entered username \"$CVEC_USERNAME\" and a ${#CVEC_PASSWORD}-character password)"
  die "Couldn't connect. Check the address/username/password with whoever gave them to you, then re-run."
fi

# --- 4. output -------------------------------------------------------------------
step "4/5  How do you want the data?"
say "stdout: one JSON line per change, printed to your terminal (pipe it anywhere)."
say "files:  one file per CVE, <dir>/<year>/<CVE-ID>.json, always the latest version."
choose CVEC_MODE "Output:" stdout stdout files
if [ "$CVEC_MODE" = files ]; then
  ask CVEC_OUT "Directory (relative paths are from $CALLER_DIR):" "~/Downloads/cvec-cves"
  case "$CVEC_OUT" in
    "~") CVEC_OUT=$HOME ;;
    "~/"*) CVEC_OUT="$HOME/${CVEC_OUT#"~/"}" ;;
    /*) ;;
    *) CVEC_OUT="$CALLER_DIR/${CVEC_OUT#./}" ;;
  esac
  ok "files will go to $CVEC_OUT"
fi
say ""
say "Where to start: 'beginning' replays everything the feed has; a date like"
say "2026-11-16 starts there. After the first run it resumes where you left off."
ask CVEC_SINCE "Start from:" "beginning"
[ "$CVEC_SINCE" = beginning ] || node -e 'process.exit(isNaN(Date.parse(process.argv[1]))?1:0)' "$CVEC_SINCE" \
  || die "\"$CVEC_SINCE\" isn't a date (try 2026-11-16 or 2026-11-16T12:00:00Z)"
CVEC_TOPIC="${CVEC_TOPIC:-cve.changes}"

# --- 5. save & start -------------------------------------------------------------
step "5/5  Saving your settings"
umask 077
{
  echo "# Written by quickstart.sh. Edit freely; see: npm start -- --help"
  echo "CVEC_BROKERS=$CVEC_BROKERS"
  echo "CVEC_USERNAME=$CVEC_USERNAME"
  echo "CVEC_PASSWORD=$CVEC_PASSWORD"
  echo "CVEC_SSL=$CVEC_SSL"
  echo "CVEC_TOPIC=$CVEC_TOPIC"
  if [ "$CVEC_MODE" = files ]; then echo "CVEC_OUT=$CVEC_OUT"; fi
} > "$APP/.env"
ok "saved to $APP/.env (only readable by you; it holds your password)"

# Later runs resume where the consumer group stopped; only this first run
# seeks to the chosen start date.
LATER_ARGS=()
if [ "$CVEC_MODE" = files ]; then LATER_ARGS+=(--quiet); fi
START_ARGS=(${LATER_ARGS[@]+"${LATER_ARGS[@]}"})
if [ "$CVEC_SINCE" != beginning ]; then START_ARGS+=(--since "$CVEC_SINCE"); fi

cat <<EOF

    To run it again later (it picks up where it stopped):
      cd $APP && npm start${LATER_ARGS[*]:+ -- ${LATER_ARGS[*]}}
    To replay from a date:  npm start -- --since 2026-11-16
    Other options:          npm start -- --help
    Stop it any time with Ctrl-C.
EOF
[ "$CVEC_MODE" = files ] && note "Files mode is quiet: watch $CVEC_OUT fill up (e.g. find $CVEC_OUT -name '*.json' | wc -l)."

choose CVEC_START "Start consuming now?" yes yes no
[ "$CVEC_START" = yes ] || { say "All set."; exit 0; }
cd "$APP"
if [ "$CVEC_MODE" = files ]; then mkdir -p "$CVEC_OUT"; fi
printf '\n'
exec node --env-file=.env --disable-warning=TimeoutNegativeWarning node-consumer.ts ${START_ARGS[@]+"${START_ARGS[@]}"}
