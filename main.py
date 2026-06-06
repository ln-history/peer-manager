import os
import time
import json
import logging
import sys
import psycopg
import ipaddress
from datetime import datetime, timezone
from pyln.client import LightningRpc
from prometheus_client import start_http_server, Gauge, Counter

# --- CONFIG ---
NODES: dict[str, str] = {
    "alice": "/rpc/alice/lightning-rpc",
    "bob":   "/rpc/bob/lightning-rpc",
}
POSTGRES_URI = os.getenv("POSTGRES_URI")
TARGET_PEERS_FILE = "/config/target_peers.json"
POLL_INTERVAL = 10
METRICS_PORT = 8003

# --- OBSERVABILITY ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stdout
)
logger = logging.getLogger("peer-manager")

ACTIVE_PEERS_GAUGE = Gauge('ln_peers_active', 'Number of currently connected peers', ['collector'])
CONNECTION_EVENTS = Counter('ln_peer_events_total', 'Total connection/disconnection events', ['collector', 'type'])
RECONNECT_ATTEMPTS = Counter('ln_peer_reconnect_attempts_total', 'Total auto-reconnection attempts', ['collector'])
RPC_ERRORS = Counter('ln_peer_rpc_errors_total', 'Total RPC failures', ['collector'])

# --- STATE ---
_node_ids: dict[str, str] = {}
_db_conn: psycopg.Connection | None = None
_targets_cache: dict = {}
_targets_mtime: float = 0.0


def get_db() -> psycopg.Connection:
    global _db_conn
    if _db_conn is None or _db_conn.closed:
        _db_conn = psycopg.connect(POSTGRES_URI, autocommit=True)
    return _db_conn


def parse_netaddr_string(netaddr_str: str) -> tuple[int, str, int] | None:
    """
    Parses 'IP:PORT' string from CLN RPC into (type_id, address, port).
    Mappings: 1=IPv4, 2=IPv6, 3=TorV2, 4=TorV3, 5=DNS
    """
    if not netaddr_str:
        return None

    try:
        if ".onion" in netaddr_str:
            parts = netaddr_str.split(':')
            addr = parts[0]
            port = int(parts[1]) if len(parts) > 1 else 9735
            type_id = 4 if len(addr) > 20 else 3
            return (type_id, addr, port)

        if '[' in netaddr_str:
            close_bracket = netaddr_str.rfind(']')
            addr = netaddr_str[1:close_bracket]
            remainder = netaddr_str[close_bracket + 1:]
            port = int(remainder[1:]) if remainder.startswith(':') else 9735
            return (2, addr, port)

        parts = netaddr_str.split(':')
        addr = parts[0]
        port = int(parts[1]) if len(parts) > 1 else 9735

        try:
            ipaddress.ip_address(addr)
            return (1, addr, port)
        except ValueError:
            return (5, addr, port)

    except Exception as e:
        logger.warning(f"Failed to parse netaddr string '{netaddr_str}': {e}")
        return None


def load_targets() -> dict:
    global _targets_cache, _targets_mtime
    try:
        mtime = os.path.getmtime(TARGET_PEERS_FILE)
        if mtime != _targets_mtime:
            with open(TARGET_PEERS_FILE, 'r') as f:
                _targets_cache = json.load(f)
            _targets_mtime = mtime
    except FileNotFoundError:
        pass
    except Exception as e:
        logger.warning(f"Failed to load targets file: {e}")
    return _targets_cache


def sync_peers(node_name: str, rpc_path: str) -> None:
    rpc = LightningRpc(rpc_path)

    # 1. Liveness Check
    try:
        info = rpc.getinfo()
        if node_name not in _node_ids:
            _node_ids[node_name] = info['id']
            logger.info(f"[{node_name}] Connected to node {info['id'][:8]}...")

        collector_id = _node_ids[node_name]
        peers_data = rpc.listpeers()['peers']

    except Exception as e:
        logger.warning(f"[{node_name}] Waiting for node... ({e})")
        RPC_ERRORS.labels(collector=node_name).inc()
        return

    active_peers = {p['id']: p for p in peers_data if p['connected']}
    ACTIVE_PEERS_GAUGE.labels(collector=node_name).set(len(active_peers))

    conn = get_db()
    try:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT peer_node_id, id
                    FROM peer_sessions
                    WHERE collector_node_id = %s AND disconnected_at IS NULL
                """, (collector_id,))
                db_active_sessions = {row[0]: row[1] for row in cur.fetchall()}

                now = datetime.now(timezone.utc)

                # 2. HANDLE DISCONNECTIONS (before new connections to avoid stale open sessions)
                for peer_id, session_db_id in db_active_sessions.items():
                    if peer_id not in active_peers:
                        logger.info(f"[{node_name}] Session Ended: {peer_id[:8]}...")
                        CONNECTION_EVENTS.labels(collector=node_name, type="disconnected").inc()

                        cur.execute("""
                            UPDATE peer_sessions
                            SET disconnected_at = %s, termination_reason = 'connection_lost'
                            WHERE id = %s
                        """, (now, session_db_id))

                # 3. HANDLE NEW CONNECTIONS
                for peer_id, p_data in active_peers.items():
                    if peer_id not in db_active_sessions:
                        logger.info(f"[{node_name}] New Session: {peer_id[:8]}...")
                        CONNECTION_EVENTS.labels(collector=node_name, type="connected").inc()

                        cur.execute("""
                            INSERT INTO observed_peers (node_id, first_connected_at, last_connected_at)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (node_id) DO UPDATE SET last_connected_at = %s
                        """, (peer_id, now, now, now))

                        cur.execute("""
                            INSERT INTO peer_sessions
                            (collector_node_id, peer_node_id, features, connected_at)
                            VALUES (%s, %s, %s, %s)
                            RETURNING id
                        """, (collector_id, peer_id, p_data.get('features'), now))

                        row = cur.fetchone()
                        assert row is not None
                        session_id = row[0]

                        for netaddr in p_data.get('netaddr', []):
                            parsed = parse_netaddr_string(netaddr)
                            if parsed:
                                type_id, addr, port = parsed
                                cur.execute("""
                                    INSERT INTO peer_addresses (session_id, type_id, address, port)
                                    VALUES (%s, %s, %s, %s)
                                """, (session_id, type_id, addr, port))

    except Exception:
        logger.exception(f"[{node_name}] DB sync failed, transaction rolled back")

    # 4. AUTO-RECONNECT (outside the DB transaction)
    targets = load_targets().get(node_name, [])
    for target in targets:
        t_id = target['id']
        t_addr = target.get('address')

        if t_id not in active_peers:
            logger.info(f"[{node_name}] Reconnecting to target {t_id[:8]}...")
            RECONNECT_ATTEMPTS.labels(collector=node_name).inc()
            try:
                connect_str = f"{t_id}@{t_addr}" if t_addr else t_id
                rpc.connect(connect_str)
            except Exception as e:
                logger.warning(f"[{node_name}] Reconnect failed: {e}")


def main() -> None:
    start_http_server(METRICS_PORT)
    logger.info(f"Metrics server started on port {METRICS_PORT}")
    logger.info("Peer Manager Started")

    while True:
        for name, rpc_path in NODES.items():
            sync_peers(name, rpc_path)
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
