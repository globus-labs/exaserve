// Minimal Pingora-based HTTP load balancer for the ExaServe
// benchmarks. Reads a YAML config that lists upstream Ray Serve nodes and
// the listen port, then proxies all requests round-robin or least-request.
//
// Single route group only (no per-model fan-out); the eval bench currently
// only exercises one model at a time. If you need multi-model routing add
// a `routes:` block to the YAML and dispatch in `upstream_peer`.

use async_trait::async_trait;
use clap::Parser;
use pingora::prelude::*;
use pingora::server::configuration::ServerConf;
use pingora::services::background::background_service;
use pingora_load_balancing::{
    health_check::TcpHealthCheck,
    selection::{LeastRequest, RoundRobin},
    LoadBalancer,
};
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(version, about = "Pingora-based HTTP LB for Ray Serve nodes")]
struct Cli {
    /// Path to the YAML config produced by exaserve.proxy.pingora_proxy.
    #[arg(short, long)]
    config: String,
    /// Parse and validate the supplied config, then exit without binding.
    #[arg(long)]
    check_config: bool,
}

#[derive(Debug, Deserialize)]
struct PingoraConfig {
    listen: String,
    /// "round_robin" or "least_request". Default round_robin.
    #[serde(default = "default_lb")]
    lb_method: String,
    upstreams: Vec<String>,
    /// Worker thread count for the Pingora server (0 = num_cpus).
    #[serde(default)]
    threads: usize,
    /// Upstream connect timeout (ms).
    #[serde(default = "default_connect_ms")]
    connect_timeout_ms: u64,
    /// Per-request timeout (ms). 0 = unbounded.
    #[serde(default = "default_request_ms")]
    request_timeout_ms: u64,
    /// Health-check interval (seconds). 0 = disabled.
    #[serde(default = "default_hc_seconds")]
    health_check_interval_s: u64,
}

fn default_lb() -> String {
    "round_robin".to_string()
}
fn default_connect_ms() -> u64 {
    5_000
}
fn default_request_ms() -> u64 {
    330_000
}
fn default_hc_seconds() -> u64 {
    5
}

// Trait-object wrapper so we can pick the selector at runtime from config.
enum Lb {
    RoundRobin(Arc<LoadBalancer<RoundRobin>>),
    LeastRequest(Arc<LoadBalancer<LeastRequest>>),
}

impl Lb {
    fn select(&self, key: &[u8]) -> Option<pingora_load_balancing::Backend> {
        match self {
            Lb::RoundRobin(b) => b.select(key, 256),
            Lb::LeastRequest(b) => b.select(key, 256),
        }
    }
}

pub struct LbCtx {
    lb: Lb,
    connect_timeout: Duration,
    request_timeout: Option<Duration>,
}

#[async_trait]
impl ProxyHttp for LbCtx {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        let upstream = self
            .lb
            .select(b"")
            .ok_or_else(|| Error::new_str("no healthy upstream"))?;
        // Plain HTTP (no TLS, no SNI) — Ray Serve speaks HTTP/1.1 on the HSN.
        let mut peer = HttpPeer::new(upstream, false, String::new());
        // Pingora's options use Duration directly.
        peer.options.connection_timeout = Some(self.connect_timeout);
        if let Some(t) = self.request_timeout {
            peer.options.read_timeout = Some(t);
            peer.options.write_timeout = Some(t);
            peer.options.total_connection_timeout = Some(t);
        }
        Ok(Box::new(peer))
    }
}

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let cli = Cli::parse();
    let raw = std::fs::read_to_string(&cli.config)
        .unwrap_or_else(|e| panic!("failed to read config {}: {}", cli.config, e));
    let cfg: PingoraConfig =
        serde_yaml::from_str(&raw).expect("failed to parse pingora config YAML");

    if cfg.upstreams.is_empty() {
        panic!("config has no upstreams");
    }
    if cfg.threads > 4096 {
        panic!("threads must be in [0, 4096]");
    }
    if cfg.connect_timeout_ms == 0 || cfg.connect_timeout_ms > 3_600_000 {
        panic!("connect_timeout_ms must be in [1, 3600000]");
    }
    if cfg.request_timeout_ms > 86_400_000 {
        panic!("request_timeout_ms must be in [0, 86400000]");
    }
    if cfg.health_check_interval_s > 3600 {
        panic!("health_check_interval_s must be in [0, 3600]");
    }
    if !matches!(cfg.lb_method.as_str(), "round_robin" | "least_request") {
        panic!("unsupported lb_method: {}", cfg.lb_method);
    }
    if cfg.listen.parse::<std::net::SocketAddr>().is_err() {
        panic!("invalid listen address: {}", cfg.listen);
    }
    for upstream in &cfg.upstreams {
        if upstream.parse::<std::net::SocketAddr>().is_err()
            && !upstream.rsplit_once(':').is_some_and(|(host, port)| {
                !host.is_empty() && port.parse::<u16>().is_ok()
            })
        {
            panic!("invalid upstream address: {}", upstream);
        }
    }
    if cli.check_config {
        log::info!("configuration valid: {}", cli.config);
        return;
    }

    // Build LB + HC; both need to live in the Pingora server.
    let mut opt = Opt::default();
    opt.daemon = false;
    let worker_threads = if cfg.threads == 0 {
        std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1)
    } else {
        cfg.threads
    };
    let server_conf = ServerConf {
        daemon: false,
        threads: worker_threads,
        ..ServerConf::default()
    };
    let mut server = Server::new_with_opt_and_conf(Some(opt), server_conf);
    server.bootstrap();

    let hc_interval = if cfg.health_check_interval_s == 0 {
        None
    } else {
        Some(Duration::from_secs(cfg.health_check_interval_s))
    };

    let lb = match cfg.lb_method.as_str() {
        "least_request" => {
            let mut lb: LoadBalancer<LeastRequest> =
                LoadBalancer::try_from_iter(&cfg.upstreams).expect("LB build (least_request)");
            lb.set_health_check(TcpHealthCheck::new());
            if let Some(int) = hc_interval {
                lb.health_check_frequency = Some(int);
            }
            let lb = Arc::new(lb);
            let hc = background_service("pingora_lb_hc", lb.clone());
            server.add_service(hc);
            Lb::LeastRequest(lb)
        }
        _ => {
            let mut lb: LoadBalancer<RoundRobin> =
                LoadBalancer::try_from_iter(&cfg.upstreams).expect("LB build (round_robin)");
            lb.set_health_check(TcpHealthCheck::new());
            if let Some(int) = hc_interval {
                lb.health_check_frequency = Some(int);
            }
            let lb = Arc::new(lb);
            let hc = background_service("pingora_lb_hc", lb.clone());
            server.add_service(hc);
            Lb::RoundRobin(lb)
        }
    };

    let connect_timeout = Duration::from_millis(cfg.connect_timeout_ms);
    let request_timeout = if cfg.request_timeout_ms == 0 {
        None
    } else {
        Some(Duration::from_millis(cfg.request_timeout_ms))
    };

    let ctx = LbCtx {
        lb,
        connect_timeout,
        request_timeout,
    };
    let mut proxy = pingora::proxy::http_proxy_service(&server.configuration, ctx);
    proxy.add_tcp(&cfg.listen);

    server.add_service(proxy);
    log::info!(
        "pingora_lb listening on {} | upstreams={} | lb={} | threads={} | connect_to={}ms req_to={}ms",
        cfg.listen,
        cfg.upstreams.len(),
        cfg.lb_method,
        worker_threads,
        cfg.connect_timeout_ms,
        cfg.request_timeout_ms,
    );
    server.run_forever();
}
