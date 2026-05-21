// Minimal Pingora-based HTTP load balancer for the Aurora Ray Serve
// benchmarks. Reads a YAML config that lists upstream Ray Serve nodes and
// the listen port, then proxies all requests round-robin or least-request.
//
// Single route group only (no per-model fan-out); the eval bench currently
// only exercises one model at a time. If you need multi-model routing add
// a `routes:` block to the YAML and dispatch in `upstream_peer`.

use async_trait::async_trait;
use clap::Parser;
use pingora::prelude::*;
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
    /// Path to the YAML config produced by aurora_rayserver.proxy.pingora_proxy.
    #[arg(short, long)]
    config: String,
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

    // Build LB + HC; both need to live in the Pingora server.
    let mut opt = Opt::default();
    opt.daemon = false;
    let mut server = Server::new(Some(opt)).expect("server init");
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

    if cfg.threads > 0 {
        // Pingora's worker count comes from the global configuration; we
        // pass threads via the ServerConf builder when constructed from
        // YAML. For simplicity, log + ignore here -- the default uses all
        // cores, which is what we want for benchmark runs anyway.
        log::warn!(
            "config requested {} worker threads, but this binary uses Pingora's \
             default thread pool sizing (all cores). Set OMP_NUM_THREADS or \
             taskset externally if needed.",
            cfg.threads
        );
    }

    server.add_service(proxy);
    log::info!(
        "pingora_lb listening on {} | upstreams={} | lb={} | connect_to={}ms req_to={}ms",
        cfg.listen,
        cfg.upstreams.len(),
        cfg.lb_method,
        cfg.connect_timeout_ms,
        cfg.request_timeout_ms,
    );
    server.run_forever();
}
