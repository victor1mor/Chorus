use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;

#[derive(Parser, Debug, Clone)]
#[command(name = "Chorus")]
#[command(author)]
#[command(version)]
#[command(about = "Concurrent HTTP(S) download manager with resumable jobs", long_about = None)]
pub struct Cli {
    /// The URLs to download
    #[arg(required_unless_present_any = ["torrent", "magnet", "resume_job", "pause_job", "cancel_job", "rpc_bind"], num_args = 1..)]
    pub urls: Vec<String>,

    /// Download a .torrent file using HTTP(S) and UDP trackers, --peer or DHT
    #[arg(long, conflicts_with_all = ["urls", "resume_job", "pause_job", "cancel_job", "sha256"])]
    pub torrent: Option<PathBuf>,

    /// Download a magnet link using HTTP(S) and UDP trackers, --peer or DHT
    #[arg(long, conflicts_with_all = ["urls", "torrent", "resume_job", "pause_job", "cancel_job", "sha256"])]
    pub magnet: Option<String>,

    /// Peer IP and port for --torrent or --magnet (repeat up to 50 times)
    #[arg(long)]
    pub peer: Vec<SocketAddr>,

    /// Write one JSON event per line to stdout
    #[arg(long)]
    pub events: bool,

    /// Resume a job by ID
    #[arg(long, conflicts_with_all = ["urls", "pause_job", "cancel_job"])]
    pub resume_job: Option<String>,

    /// Pause a job by ID
    #[arg(long, conflicts_with_all = ["urls", "resume_job", "cancel_job"])]
    pub pause_job: Option<String>,

    /// Cancel a job by ID
    #[arg(long, conflicts_with_all = ["urls", "resume_job", "pause_job"])]
    pub cancel_job: Option<String>,

    /// Remove partial files when cancelling a job
    #[arg(long, requires = "cancel_job")]
    pub remove_partial: bool,

    /// Number of concurrent connections per download
    #[arg(short = 'x', long, default_value_t = 4)]
    pub connections: usize,

    /// Output file name for HTTP, or destination directory for --torrent or --magnet
    #[arg(short = 'o', long)]
    pub output: Option<String>,

    /// Retry attempts per chunk after the first failure
    #[arg(long, default_value_t = 3)]
    pub retries: usize,

    /// Maximum ranged piece size in bytes
    #[arg(long, default_value_t = 8 * 1024 * 1024)]
    pub piece_size: u64,

    /// Ignore and delete any existing partial download state
    #[arg(long)]
    pub no_resume: bool,

    /// Fail if the final output file already exists
    #[arg(long)]
    pub no_overwrite: bool,

    /// Expected SHA-256 hex digest of the completed file
    #[arg(long)]
    pub sha256: Option<String>,

    /// Connection timeout in milliseconds; 0 disables it
    #[arg(long, default_value_t = 10_000)]
    pub connect_timeout_ms: u64,

    /// Per-read timeout in milliseconds; 0 disables it
    #[arg(long, default_value_t = 30_000)]
    pub read_timeout_ms: u64,

    /// Total request timeout in milliseconds; 0 disables it
    #[arg(long, default_value_t = 0)]
    pub timeout_ms: u64,

    /// Directory for safe, redacted job lifecycle manifests
    #[arg(long)]
    pub job_dir: Option<PathBuf>,

    /// Maximum number of concurrent jobs globally
    #[arg(short = 'j', long, default_value_t = 4)]
    pub max_jobs: usize,

    /// Maximum number of concurrent jobs per host
    #[arg(long, default_value_t = 2)]
    pub max_jobs_per_host: usize,

    /// Limit download speed per job in bytes per second (0 means unlimited)
    #[arg(long, default_value_t = 0)]
    pub limit_rate: u64,

    /// Limit overall download speed in bytes per second (0 means unlimited)
    #[arg(long, default_value_t = 0)]
    pub max_overall_download_limit: u64,

    /// Disable the DHT (Distributed Hash Table) for discovering peers
    #[arg(long)]
    pub disable_dht: bool,

    /// DHT bootstrap host and port (repeatable)
    #[arg(long, value_name = "host:port", default_values = [
        "dht.libtorrent.org:25401", "router.bittorrent.com:6881", "dht.transmissionbt.com:6881"
    ])]
    pub dht_bootstrap: Vec<String>,

    /// Bind address and port for the JSON-RPC/WebSocket server
    #[arg(long)]
    pub rpc_bind: Option<String>,

    /// Root directory for RPC downloads (defaults to the server's current directory)
    #[arg(long, value_name = "PASTA")]
    pub download_dir: Option<PathBuf>,

    /// Allow RPC jobs to reach loopback and private addresses (local testing only)
    #[arg(long)]
    pub allow_private_network: bool,

    /// Authentication token required for non-local RPC/WebSocket connections
    #[arg(long)]
    pub rpc_auth_token: Option<String>,

    /// Allow non-local RPC/WebSocket connections without TLS; still requires --rpc-auth-token and only over WebSocket
    #[arg(long)]
    pub allow_remote_insecure: bool,
}

impl Cli {
    pub fn parse_args() -> Self {
        let args = Self::parse();

        if !args.peer.is_empty() && args.torrent.is_none() && args.magnet.is_none() {
            eprintln!("Error: --peer requires --torrent or --magnet");
            std::process::exit(1);
        }
        if args.peer.len() > 50 {
            eprintln!("Error: at most 50 --peer values are allowed");
            std::process::exit(1);
        }
        if args.max_jobs == 0 {
            eprintln!("Error: --max-jobs must be at least 1");
            std::process::exit(1);
        }
        if args.max_jobs_per_host == 0 {
            eprintln!("Error: --max-jobs-per-host must be at least 1");
            std::process::exit(1);
        }
        if args.max_jobs_per_host > args.max_jobs {
            eprintln!("Error: --max-jobs-per-host cannot be greater than --max-jobs");
            std::process::exit(1);
        }

        args
    }
}
