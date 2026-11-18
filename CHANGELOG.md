# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.0] - 2026-11-18

### Added

- Download from up to 50 peers at the same time, several pieces per peer, with a request queue sized to 3 s of each peer's measured speed (5 to 250 requests).
- Fast extension (BEP 6) and the peer's `reqq` limit (BEP 10).
- Peer exchange (BEP 11) on public torrents, through the same address filter as trackers and DHT. Never on private torrents.
- IPv6 peers (BEP 7) from HTTP and UDP trackers.
- Web seeds (BEP 19) over the protected HTTP client, with SHA-1 checked before any write.

### Changed

- The bandwidth limit applies before each request instead of after each block. A peer that chokes us is no longer dropped; blocks have a 20 s deadline and silent peers a 120 s one.
- A peer that sends corrupt pieces is banned for the whole download (by IP and port; three banned ports ban the IP).

### BitTorrent limits

- Uploading and seeding are unavailable; Chorus never uploads pieces. DHT discovery is IPv4-only. Pure v2 torrents are rejected, and hybrid torrents use their v1 data.
- Web seeds use HTTP(S) only; FTP, BEP 17 `httpseeds` and magnet `ws=` are not supported.

## [0.3.0] - 2026-11-14

### Added

- Download BitTorrent jobs from `.torrent` files and magnet links (BEP 3, 9 and 10), using HTTP(S) and UDP trackers (BEP 15) or read-only DHT discovery (BEP 5 and 43).
- Resume interrupted BitTorrent downloads and recheck saved pieces with SHA-1.
- Submit `.torrent` files and magnet links through JSON-RPC `add_job`.

### Security

- Validate torrent metadata and block coordinates and lengths before accepting data. Leave a peer after three corrupt pieces and reject symbolic links in torrent output paths.
- Apply the RPC private-network restrictions to trackers, DHT nodes, and discovered peers. Chorus never uploads pieces.
- Version 0.2.1 wrote a fixed filename, `chorus_dht_cache.bin`, in the shared temporary directory on each run with DHT enabled by default. Version 0.3.0 keeps the cache only at `<job-dir>/dht-nodes.dat` and never follows symbolic links at that path.

### Changed

- Upgrade SHA-1 and SHA-256 dependencies to the RustCrypto 0.11 series.
- Pin CI actions to commit SHAs, restrict workflow permissions to read-only contents, and use the lockfile for Clippy and tests.

### BitTorrent limits

- Each download connects to one peer at a time. Automatic peer discovery is IPv4-only. Pure v2 torrents are rejected, and hybrid torrents use their v1 data.
- Uploading and seeding are unavailable. DHT is enabled by default and contacts public bootstrap nodes for discovery. Use `--disable-dht` to turn off DHT traffic.

## [0.2.1] - 2026-11-13

### Fixed

- Write the resume range file atomically (temporary file + rename) so an interrupted process no longer leaves it empty or truncated and loses resume progress.

## [0.2.0] - 2026-11-12

### Added

- Added versioned NDJSON output on stdout with `--events`.

### Changed

- Removed the interactive progress bar.
- Restored passing formatting and Clippy checks.

### Fixed

- Create the output file for empty HTTP responses and stream downloads of unknown size to completion.
- Serialize job registry updates across threads and processes to prevent lost jobs, and report persistence failures.

### Security

- Updated dependencies to clear advisories reported by `cargo audit` and `cargo deny`.
- Limit RPC to local connections without an auth token. Remote access requires authenticated WebSocket.
- Restrict `add_job` to HTTP(S) and output paths under the download directory. RPC jobs block access to private and local networks by default, including after DNS resolution and redirects.
- Redact URL credentials, queries, and fragments from output and keep RPC tokens out of output.

An earlier 1.0.0 entry was never released and described features that did not work; it was removed.
