# Chorus

![Chorus](Chorus.png)

Chorus is a concurrent HTTP(S) and BitTorrent downloader written in Rust. HTTP(S) downloads support byte ranges, retries, safe resume, and optional SHA-256 verification. BitTorrent downloads support `.torrent` files, magnet links, and resume with piece verification.

## Use

```powershell
cargo run --release -- -x 8 --sha256 <sha256> -o archive.zip https://example.com/archive.zip
```

Partial downloads are stored beside the output as `.part`, `.part.meta`, and `.part.ranges`. Chorus resumes them when the server supplies a strong ETag or `Last-Modified` validator; use `--no-resume` to discard that state. Use `--no-overwrite` to keep an existing final output untouched.

Use `--job-dir .\jobs` to write a versioned JSON manifest for the current
transfer. It records `active` before the download and `complete` only after
the final file is in place; URLs are redacted before being persisted.

Run `cargo run -- --help` for every option.

## BitTorrent

```powershell
Chorus --torrent .\example.torrent --job-dir .\jobs -o .\downloads
Chorus --magnet "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567" --job-dir .\jobs -o .\downloads
```

Chorus supports HTTP(S) and UDP trackers and read-only DHT discovery. DHT is
enabled by default and contacts public bootstrap nodes when used for discovery.
Use `--disable-dht` to turn off DHT traffic or repeat `--dht-bootstrap host:port`
to supply your own bootstrap nodes. Private torrents require a usable tracker or
an explicit CLI `--peer`.

Each download uses up to 50 peers at once, plus web seeds (BEP 19). Peers come
from trackers (IPv4 and IPv6), DHT (IPv4), and PEX on public torrents. Pure v2 torrents are rejected, and hybrid torrents use their v1 data.
Uploading and seeding are unavailable.

## Requirements

- Rust 1.99.1 (pinned in rust-toolchain.toml). Install via [rustup](https://rustup.rs/).

## Install

No binary is published. Build from source with `cargo build --release --locked`.

## Machine-readable output

Use `--events` to emit NDJSON on stdout. See the [v1 event contract](docs/EVENTOS.md).

## Remote control

Chorus exposes JSON-RPC 2.0 over WebSocket or newline-delimited JSON. Without
`--rpc-auth-token`, it accepts only local connections. Remote connections require
`--allow-remote-insecure`, WebSocket and a valid bearer token. TLS is unavailable.

`add_job` takes an `output` path relative to the download directory and exactly
one source field: `urls` (HTTP(S) URL array), `torrent` (standard base64 with
at most 4 MiB of decoded `.torrent` data), or `magnet` (magnet URI). The RPC
rejects `peers`. Torrents and magnets can omit a tracker when DHT is enabled.
A private torrent submitted via RPC requires a usable tracker. RPC jobs block
access to private and local networks by default.

## Updates and Rollback

To update or roll back, build the desired version from source and replace the `Chorus` binary.

## Verify

```powershell
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
$auditDb = Join-Path ([IO.Path]::GetTempPath()) ("chorus-audit-" + [guid]::NewGuid())
cargo audit --db $auditDb
```
