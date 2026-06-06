# peer-manager

Component is part of the [ln-history](ln-history.info) project.

Monitor the connected peers of one or multiple core lightning nodes (currently 2: Alice and Bob)
and store the session data into the ln-history-database

## Configuration
You can update and specify an `initial` node that the peer-manager will try 
to make one node to connect to, see the `config/target_peers.json` file:

```json
{
  "alice": [
    {
      "id": "node_id",
      "address": "ip_address:port",
      "note": "Bob"
    }
  ],
  "bob": []
}
```