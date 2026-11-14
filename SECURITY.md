# Security and supported scope

Chorus is a pre-1.0 HTTP(S) and BitTorrent downloader, distributed from this Git repository as source
code. It has not received an independent security audit. Automated checks do not guarantee the absence
of vulnerabilities. See CHANGELOG.md for what each release changed and its known limits.

## Trust boundary

- Every BitTorrent peer, tracker and DHT node is treated as hostile. Torrent metadata, block offsets and
  lengths are validated before data is accepted, every piece is checked with SHA-1 before it is written,
  and a peer that keeps sending corrupt pieces or unrequested blocks is dropped.
- Chorus never uploads pieces. DHT runs in read-only mode (BEP 43) and never answers queries.
- DHT is enabled by default and contacts public bootstrap nodes. Use `--disable-dht` to keep a job off
  the DHT. Private torrents (BEP 27) never use DHT.
- The JSON-RPC control plane accepts only local connections unless you pass `--rpc-auth-token`. Remote
  access additionally requires `--allow-remote-insecure`, WebSocket and the bearer token. TLS is not
  available: do not expose the RPC port to untrusted networks.
- RPC jobs cannot reach private, loopback or link-local addresses (including through DNS, redirects,
  trackers, DHT nodes and peers) unless the daemon runs with `--allow-private-network`.
- Output paths given over RPC must stay inside the download directory. Symbolic links in torrent output
  paths are rejected.
- Local administrators and processes running as your user are inside the trust boundary. Use job and
  download directories that only you control.

## Reporting a vulnerability

Report vulnerabilities privately to the repository owner through GitHub. Do not open a public issue with
exploit details. No paid bounty or response time is promised.
