mod auth;
mod block_constraints;
mod blocks;
mod budgets;
mod bytes;
mod chain;
mod chains;
mod client_query;
mod config;
mod errors;
mod exchange_rate;
mod graphql;
mod indexer_client;
mod indexing_performance;
mod kafka;
mod metrics;
mod middleware;
mod network;
mod receipts;
mod reports;
mod time;
mod unattestable_errors;
mod version;

use std::{
    collections::HashSet,
    env,
    io::Write as _,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use auth::AuthContext;
use axum::{
    Router,
    http::{self, status::StatusCode},
    routing,
    serve::ListenerExt,
};
use budgets::{Budgeter, USD};
use chains::Chains;
use client_query::context::Context;
use config::{ApiKeys, BlocklistEntry, ExchangeRateProvider};
use indexer_client::IndexerClient;
use indexing_performance::IndexingPerformance;
use middleware::{
    RequestTracingLayer, RequireAuthorizationLayer, create_x402_layer, legacy_auth_adapter,
    x402_auth_adapter,
};
use network::{indexer_blocklist, subgraph_client::Client as SubgraphClient};
use prometheus::{self, Encoder as _};
use rand::Rng as _;
use receipts::ReceiptSigner;
use thegraph_core::{
    alloy::{dyn_abi::Eip712Domain, primitives::ChainId, signers::local::PrivateKeySigner},
    attestation,
};
use tokio::{net::TcpListener, signal::unix::SignalKind, sync::watch};
use tower_http::cors::{self, CorsLayer};
use tracing_subscriber::{EnvFilter, prelude::*};

use crate::config::KafkaConfig;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[tokio::main]
async fn main() {
    let conf_path = env::args()
        .nth(1)
        .expect("Missing argument for config path")
        .parse::<PathBuf>()
        .unwrap();
    let conf = config::load_from_file(&conf_path).expect("Failed to load config");

    let receipt_signer = PrivateKeySigner::from_bytes(&conf.receipts.signer)
        .expect("failed to prepare receipt signer");
    let signer_address = receipt_signer.address();

    init_logging("graph-gateway", conf.log_json);
    tracing::info!("gateway ID: {:?}", signer_address);

    let setup_termination = tokio::spawn(async {
        await_shutdown_signals().await;
        tracing::warn!("shutdown");
        std::process::exit(1);
    });

    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .unwrap();
    let kafka_consumer = KafkaConsumer::new(conf.kafka.clone());

    let grt_per_usd = match conf.exchange_rate_provider {
        ExchangeRateProvider::Fixed(grt_per_usd) => watch::channel(grt_per_usd).1,
        ExchangeRateProvider::Rpc(url) => exchange_rate::grt_per_usd(url).await,
    };

    let attestation_domain: &'static Eip712Domain =
        Box::leak(Box::new(attestation::eip712_domain(
            conf.attestations
                .chain_id
                .parse::<ChainId>()
                .expect("failed to parse attestation domain chain_id"),
            conf.attestations.dispute_manager,
        )));

    let indexer_client = IndexerClient {
        client: http_client.clone(),
        max_response_size: conf.max_indexer_response_size,
    };
    let network_subgraph_client = SubgraphClient {
        client: indexer_client.clone(),
        indexers: conf.trusted_indexers,
        latest_block: None,
        page_size: 500,
        max_lag_seconds: conf.network_subgraph_max_lag_seconds,
    };
    let indexer_host_blocklist = match &conf.ip_blocker_db {
        Some(path) => {
            config::load_ip_blocklist_from_file(path).expect("failed to load IP blocker DB")
        }
        None => Default::default(),
    };
    let topic_env = conf.kafka_topic_environment.as_deref();
    let topic_name = |base: &str| config::topic_name(topic_env, base);

    let indexer_blocklist = indexer_blocklist::Blocklist::spawn(
        conf.blocklist,
        kafka_consumer.clone(),
        topic_name("gateway_blocklist"),
    );
    let version_info =
        version::VersionInfo::new(&conf.min_indexer_version, &conf.min_graph_node_version);
    let mut network = network::service::spawn(
        http_client.clone(),
        network_subgraph_client,
        indexer_blocklist.clone(),
        conf.min_indexer_version,
        conf.min_graph_node_version,
        indexer_host_blocklist,
    );
    let indexing_perf = IndexingPerformance::new(network.clone());
    network.wait_until_ready().await;

    let receipt_signer: &'static ReceiptSigner = Box::leak(Box::new(ReceiptSigner::new(
        conf.receipts.payer,
        receipt_signer,
        conf.receipts.chain_id,
        conf.receipts.verifier,
        conf.subgraph_service,
    )));

    let auth_service = init_auth_service(
        http_client.clone(),
        &kafka_consumer,
        conf.api_keys,
        conf.payment_required,
    )
    .await
    .expect("failed to start auth service");

    let budgeter: &'static Budgeter =
        Box::leak(Box::new(Budgeter::new(USD(conf.query_fees_target))));

    let reporter = reports::Reporter::create(
        signer_address,
        conf.graph_env_id,
        reports::Topics {
            queries: topic_name("gateway_queries"),
            attestations: topic_name("gateway_attestations"),
        },
        conf.kafka,
    )
    .unwrap();

    let ctx = Context {
        indexer_client,
        receipt_signer,
        budgeter,
        chains: Box::leak(Box::new(Chains::new(conf.chain_aliases))),
        grt_per_usd,
        indexing_perf,
        network,
        attestation_domain,
        reporter,
    };

    let blocklist: watch::Receiver<Vec<BlocklistEntry>> = indexer_blocklist.blocklist;

    // Host metrics on a separate server with a port that isn't open to public requests.
    tokio::spawn(async move {
        let router = Router::new().route("/metrics", routing::get(handle_metrics));
        let metrics_listener = TcpListener::bind(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
            conf.port_metrics,
        ))
        .await
        .expect("Failed to bind metrics server");
        axum::serve(metrics_listener, router.into_make_service())
            .await
            .expect("Failed to start metrics server");
    });

    let api = Router::new()
        .route(
            "/deployments/id/{deployment_id}",
            routing::post(client_query::handle_query),
        )
        .route(
            "/deployments/id/{deployment_id}/indexers/id/{indexer}",
            routing::post(client_query::handle_indexer_query),
        )
        .route(
            "/subgraphs/id/{subgraph_id}",
            routing::post(client_query::handle_query),
        )
        .route(
            "/{api_key}/deployments/id/{deployment_id}",
            routing::post(client_query::handle_query),
        )
        .route(
            "/{api_key}/subgraphs/id/{subgraph_id}",
            routing::post(client_query::handle_query),
        )
        .with_state(ctx.clone())
        .layer(
            // ServiceBuilder works by composing all layers into one such that they run top to
            // bottom, and then the response would bubble back up through the layers in reverse
            tower::ServiceBuilder::new()
                .layer(
                    CorsLayer::new()
                        .allow_origin(cors::Any)
                        .allow_headers(cors::Any)
                        .allow_methods([http::Method::OPTIONS, http::Method::POST]),
                )
                .layer(RequestTracingLayer::new(format!("{signer_address:?}")))
                .layer(axum::middleware::from_fn(legacy_auth_adapter))
                .layer(RequireAuthorizationLayer::new(auth_service)),
        );

    let mut router = Router::new()
        .route("/", routing::get(|| async { "Ready to roll!" }))
        // This path is required by NGINX ingress controller
        .route("/ready", routing::get(|| async { "Ready" }))
        .route("/version", version::route(version_info))
        .route(
            "/blocklist",
            routing::get(move || async move { axum::Json(blocklist.borrow().clone()) }),
        )
        .route(
            "/SKILL.md",
            routing::get(|| async { include_str!("../SKILL.md") }),
        )
        .nest("/api", api);

    // x402 payment endpoints at /api/x402/*
    // Note: This path overlaps with legacy /api/{api_key}/* routes. Axum routes literal
    // segments before parameters, so /api/x402/* matches here first. This effectively
    // reserves "x402" as an API key value when x402 is enabled.
    if let Some(x402_config) = conf.x402 {
        let x402_api = Router::new()
            .route(
                "/subgraphs/id/{subgraph_id}",
                routing::post(client_query::handle_query),
            )
            .route(
                "/deployments/id/{deployment_id}",
                routing::post(client_query::handle_query),
            )
            .route(
                "/deployments/id/{deployment_id}/indexers/id/{indexer}",
                routing::post(client_query::handle_indexer_query),
            )
            .with_state(ctx)
            .layer(
                tower::ServiceBuilder::new()
                    .layer(
                        CorsLayer::new()
                            .allow_origin(cors::Any)
                            .allow_headers(cors::Any)
                            .allow_methods([http::Method::OPTIONS, http::Method::POST]),
                    )
                    .layer(RequestTracingLayer::new(format!("{signer_address:?}")))
                    .layer(create_x402_layer(&x402_config))
                    .layer(axum::middleware::from_fn(x402_auth_adapter)),
            );
        router = router.nest("/api/x402", x402_api);
        tracing::info!("x402 payment endpoints enabled at /api/x402/*");
    }

    // handle graceful shutdown via the axum server instead
    setup_termination.abort();

    let app_listener = TcpListener::bind(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
        conf.port_api,
    ))
    .await
    .expect("Failed to bind API server")
    // disable Nagle's algorithm
    .tap_io(|stream| {
        let _ = stream.set_nodelay(true);
    });
    axum::serve(
        app_listener,
        router.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(await_shutdown_signals())
    .await
    .expect("Failed to start API server");
    tracing::warn!("shutdown");
}

async fn await_shutdown_signals() {
    #[cfg(unix)]
    let sigint = async {
        tokio::signal::unix::signal(SignalKind::interrupt())
            .expect("install SIGINT handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let sigint = std::future::pending::<()>();

    #[cfg(unix)]
    let sigterm = async {
        tokio::signal::unix::signal(SignalKind::terminate())
            .expect("install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let sigterm = std::future::pending::<()>();

    tokio::select! {
        _ = sigint => (),
        _ = sigterm => (),
    }
}

async fn handle_metrics() -> impl axum::response::IntoResponse {
    let encoder = prometheus::TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    if let Err(metrics_encode_err) = encoder.encode(&metric_families, &mut buffer) {
        tracing::error!(%metrics_encode_err);
        buffer.clear();
        write!(&mut buffer, "Failed to encode metrics").unwrap();
        return (StatusCode::INTERNAL_SERVER_ERROR, Default::default());
    }
    (StatusCode::OK, String::from_utf8(buffer).unwrap())
}

pub fn init_logging(executable_name: &str, json: bool) {
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::try_new(format!("info,{executable_name}=debug")).unwrap());

    let log_default_layer = (!json).then(tracing_subscriber::fmt::layer);
    let log_json_layer = json.then(|| {
        tracing_subscriber::fmt::layer()
            .json()
            .with_current_span(false)
    });

    tracing_subscriber::registry()
        .with(env_filter)
        .with(log_default_layer)
        .with(log_json_layer)
        .init();
}

/// Creates a new [`AuthContext`] from the given configuration.
///
/// This functions awaits the completion of the initial API keys fetch.
async fn init_auth_service(
    http: reqwest::Client,
    kafka: &KafkaConsumer,
    config: ApiKeys,
    payment_required: bool,
) -> anyhow::Result<AuthContext> {
    let special_api_keys = match &config {
        ApiKeys::Endpoint { special, .. } => Arc::new(HashSet::from_iter(special.clone())),
        ApiKeys::KakfaTopic { special, .. } => Arc::new(HashSet::from_iter(special.clone())),
        ApiKeys::Fixed(_) => Default::default(),
    };

    let api_keys = match config {
        ApiKeys::Endpoint { url, auth, .. } => auth::studio_api::api_keys(http, url, auth).await,
        ApiKeys::KakfaTopic {
            topic,
            bootstrap_url,
            bootstrap_auth,
            ..
        } => auth::kafka::api_keys(kafka, &topic, &bootstrap_url, &bootstrap_auth).await?,
        ApiKeys::Fixed(api_keys) => {
            let api_keys = api_keys.into_iter().map(|k| (k.key.clone(), k)).collect();
            watch::channel(api_keys).1
        }
    };

    Ok(AuthContext {
        payment_required,
        api_keys,
        special_api_keys,
    })
}

#[derive(Clone)]
struct KafkaConsumer {
    config: KafkaConfig,
    group_id: String,
}

impl KafkaConsumer {
    pub fn new(config: KafkaConfig) -> Self {
        let group_id = format!("gateway-{:x}", rand::rng().next_u64());
        Self { config, group_id }
    }

    pub fn stream_consumer(
        &self,
        topic: &str,
    ) -> anyhow::Result<rdkafka::consumer::StreamConsumer> {
        let consumer: rdkafka::consumer::StreamConsumer =
            rdkafka::config::ClientConfig::from(self.config.clone())
                .remove("compression.codec")
                .remove("queue.buffering.max.messages")
                .remove("queue.buffering.max.ms")
                .remove("message.timeout.ms")
                .set("group.id", &self.group_id)
                .set("auto.offset.reset", "beginning")
                .create()?;
        kafka::assign_partitions(&consumer, topic, rdkafka::Offset::Beginning)?;
        Ok(consumer)
    }
}
