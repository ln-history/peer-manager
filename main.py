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
NODES = {
    "alice": {"rpc": "/rpc/alice/lightning-rpc", "id": None},
    "bob":   {"rpc": "/rpc/bob/lightning-rpc", "id": None}
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

def get_db():
    return psycopg.connect(POSTGRES_URI, autocommit=True)

def parse_netaddr_string(netaddr_str):
    """
    Parses 'IP:PORT' string from CLN RPC into (type_id, address, port).
    This mimics the logic of lnhistoryclient but for string inputs.
    
    Mappings: 1=IPv4, 2=IPv6, 3=TorV2, 4=TorV3, 5=DNS
    """
    if not netaddr_str: return None
    
    try:
        # Handle Tor (No brackets, might have port)
        if ".onion" in netaddr_str:
            parts = netaddr_str.split(':')
            addr = parts[0]
            port = int(parts[1]) if len(parts) > 1 else 9735
            # Heuristic: V3 onions are 56 chars long, V2 are 16
            type_id = 4 if len(addr) > 20 else 3 
            return (type_id, addr, port)
            
        # Handle IPv6 [::1]:9735
        if '[' in netaddr_str:
            close_bracket = netaddr_str.rfind(']')
            addr = netaddr_str[1:close_bracket]
            port = int(netaddr_str[close_bracket+2:])
            return (2, addr, port)
            
        # Handle IPv4 / DNS
        parts = netaddr_str.split(':')
        addr = parts[0]
        port = int(parts[1]) if len(parts) > 1 else 9735
        
        try:
            ipaddress.ip_address(addr)
            return (1, addr, port) # IPv4
        except ValueError:
            return (5, addr, port) # DNS
            
    except Exception as e:
        logger.warning(f"Failed to parse netaddr string '{netaddr_str}': {e}")
        return None

def load_targets():
    if os.path.exists(TARGET_PEERS_FILE):
        try:
            with open(TARGET_PEERS_FILE, 'r') as f:
                return json.load(f)
        except:
            return {}
    return {}

def sync_peers(node_name, node_config):
    rpc = LightningRpc(node_config['rpc'])
    
    # 1. Liveness Check
    try:
        info = rpc.getinfo()
        if not node_config['id']:
            node_config['id'] = info['id']
            logger.info(f"[{node_name}] Connected to node {node_config['id'][:8]}...")
            
        collector_id = node_config['id']
        peers_data = rpc.listpeers()['peers']
        
    except Exception as e:
        logger.warning(f"[{node_name}] Waiting for node... ({e})")
        RPC_ERRORS.labels(collector=node_name).inc()
        return

    active_peers = {p['id']: p for p in peers_data if p['connected']}
    ACTIVE_PEERS_GAUGE.labels(collector=node_name).set(len(active_peers))

    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT peer_node_id, id 
            FROM peer_sessions 
            WHERE collector_node_id = %s AND disconnected_at IS NULL
        """, (collector_id,))
        db_active_sessions = {row[0]: row[1] for row in cur.fetchall()}
        
        now = datetime.now(timezone.utc)

        # 2. HANDLE NEW CONNECTIONS
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
                
                session_id = cur.fetchone()[0]
                
                # C. Insert Addresses
                # Note: We iterate through the string list provided by CLN RPC
                for netaddr in p_data.get('netaddr', []):
                    parsed = parse_netaddr_string(netaddr)
                    if parsed:
                        type_id, addr, port = parsed
                        cur.execute("""
                            INSERT INTO peer_addresses (session_id, type_id, address, port)
                            VALUES (%s, %s, %s, %s)
                        """, (session_id, type_id, addr, port))

        # 3. HANDLE DISCONNECTIONS
        for peer_id, session_db_id in db_active_sessions.items():
            if peer_id not in active_peers:
                logger.info(f"[{node_name}] Session Ended: {peer_id[:8]}...")
                CONNECTION_EVENTS.labels(collector=node_name, type="disconnected").inc()
                
                cur.execute("""
                    UPDATE peer_sessions 
                    SET disconnected_at = %s, termination_reason = 'connection_lost'
                    WHERE id = %s
                """, (now, session_db_id))

    # 4. AUTO-RECONNECT
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

    conn.close()

def main():
    start_http_server(METRICS_PORT)
    logger.info(f"📈 Metrics server started on port {METRICS_PORT}")
    logger.info("🚀 Peer Manager Started")
    
    while True:
        for name, config in NODES.items():
            sync_peers(name, config)
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()