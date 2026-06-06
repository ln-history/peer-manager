# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`peer-manager` is a component of the [ln-history](https://ln-history.info) project. It polls one or more Core Lightning (CLN) nodes via Unix socket RPC, records peer connection/disconnection events into a shared PostgreSQL database, and auto-reconnects to a configurable list of target peers. Prometheus metrics are exposed on port 8003.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run locally (requires .env with POSTGRES_URI and accessible RPC sockets)
python main.py

# Build Docker image
docker build -t peer-manager .

# Lint (ruff auto-fixes, then black)
ruff check --fix main.py
black main.py

# Type check
mypy main.py

# Bump version (uses commitizen)
cz bump
```

There are no tests in this project currently.

## Architecture

Everything lives in `main.py` — there are no modules or packages. The main loop calls `sync_peers()` for each configured node every 10 seconds.

**`sync_peers(node_name, node_config)`** does four things in sequence:
1. **Liveness check** — connects to the CLN RPC socket and calls `getinfo` / `listpeers`
2. **New connections** — compares live peers against `peer_sessions` rows with no `disconnected_at`; inserts new rows into `observed_peers`, `peer_sessions`, and `peer_addresses`
3. **Disconnections** — finds sessions open in DB but absent from live peers; sets `disconnected_at` and `termination_reason`
4. **Auto-reconnect** — reads `config/target_peers.json` and calls `rpc.connect()` for any target not currently connected

**Database tables written to:** `observed_peers`, `peer_sessions`, `peer_addresses` — schema is owned by the broader ln-history database, not this repo.

**`parse_netaddr_string()`** converts CLN's `netaddr` strings (e.g. `1.2.3.4:9735`, `[::1]:9735`, `abc.onion:9735`) into `(type_id, address, port)` tuples matching the ln-history address type convention (1=IPv4, 2=IPv6, 3=TorV2, 4=TorV3, 5=DNS).

## Configuration

| Source | Purpose |
|--------|---------|
| `POSTGRES_URI` env var | PostgreSQL connection string |
| `NODES` dict in `main.py` | CLN node names and RPC socket paths (`/rpc/<name>/lightning-rpc`) |
| `config/target_peers.json` | Per-node list of peers to auto-reconnect to (keyed by node name) |

The `.env` file is loaded implicitly by `python-dotenv` (see `requirements.txt`); add `POSTGRES_URI` there for local development.

## Code Style

Line length: 120 (`black` + `ruff`). Use `ruff` for linting/import sorting, `black` for formatting, `mypy` with strict mode for type checking. Commits follow Conventional Commits (`cz_conventional_commits`).
