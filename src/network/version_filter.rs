use std::collections::HashMap;

use parking_lot::RwLock;
use semver::Version;
use thegraph_graphql_http::http_client::ReqwestExt;
use url::Url;

use crate::errors::UnavailableReason;

pub struct MinimumVersionRequirements {
    pub indexer_service: Version,
    pub graph_node: Version,
}

pub struct VersionFilter {
    http: reqwest::Client,
    min_versions: MinimumVersionRequirements,
    indexer_service_version_cache: RwLock<HashMap<String, Version>>,
    graph_node_version_cache: RwLock<HashMap<String, Version>>,
}

impl VersionFilter {
    pub fn new(http: reqwest::Client, min_versions: MinimumVersionRequirements) -> Self {
        Self {
            http,
            min_versions,
            indexer_service_version_cache: Default::default(),
            graph_node_version_cache: Default::default(),
        }
    }

    pub async fn check(&self, url: &Url) -> Result<(), UnavailableReason> {
        let indexer_service_version = self.fetch_indexer_service_version(url).await?;
        if indexer_service_version < self.min_versions.indexer_service {
            return Err(UnavailableReason::NotSupported(
                "indexer-service version below minimum".to_string(),
            ));
        }

        // An indexer whose graph-node doesn't report its version can't be shown to meet the
        // minimum, so it doesn't receive queries.
        let graph_node_version = self.fetch_graph_node_version(url).await?;
        if graph_node_version < self.min_versions.graph_node {
            return Err(UnavailableReason::NotSupported(
                "graph-node version below minimum".to_string(),
            ));
        }

        Ok(())
    }

    async fn fetch_indexer_service_version(&self, url: &Url) -> Result<Version, UnavailableReason> {
        if let Some(version) = self.indexer_service_version_cache.read().get(url.as_str()) {
            return Ok(version.clone());
        }

        let url = url
            .join("version")
            .map_err(|_| UnavailableReason::invalid_url())?;
        #[derive(Debug, serde::Deserialize)]
        struct Response {
            version: Version,
        }
        let version = self
            .http
            .get(url.clone())
            .send()
            .await
            .map_err(|_| UnavailableReason::NoStatus("indexer not available".to_string()))?
            .json::<Response>()
            .await
            .map_err(|_| UnavailableReason::NoStatus("indexer not available".to_string()))?
            .version;

        self.indexer_service_version_cache
            .write()
            .insert(url.to_string(), version.clone());

        Ok(version)
    }

    async fn fetch_graph_node_version(&self, url: &Url) -> Result<Version, UnavailableReason> {
        if let Some(version) = self.graph_node_version_cache.read().get(url.as_str()) {
            return Ok(version.clone());
        }

        let url = url
            .join("status")
            .map_err(|_| UnavailableReason::invalid_url())?;
        #[derive(Debug, serde::Deserialize)]
        struct Response {
            version: InnerVersion,
        }
        #[derive(Debug, serde::Deserialize)]
        struct InnerVersion {
            version: Version,
        }
        let version = self
            .http
            .post(url.clone())
            .send_graphql::<Response>("{ version { version } }")
            .await
            .map_err(|err| {
                UnavailableReason::NoStatus(format!("failed to query graph-node version: {err}"))
            })?
            .map_err(|err| {
                UnavailableReason::NoStatus(format!("failed to query graph-node version: {err}"))
            })?
            .version
            .version;

        self.graph_node_version_cache
            .write()
            .insert(url.to_string(), version.clone());

        Ok(version)
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use axum::{Json, Router, routing};
    use serde_json::{Value, json};
    use tokio::net::TcpListener;

    use super::*;

    /// Serves an indexer-service that reports `indexer_service` at `/version` and answers `/status`
    /// with `status`, returning its base URL.
    async fn indexer(indexer_service: &'static str, status: Value) -> Url {
        let router = Router::new()
            .route(
                "/version",
                routing::get(move || async move { Json(json!({ "version": indexer_service })) }),
            )
            .route(
                "/status",
                routing::post(move || async move { Json(status) }),
            );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        format!("http://{addr}/").parse().unwrap()
    }

    fn graph_node_reports(version: &str) -> Value {
        json!({ "data": { "version": { "version": version } } })
    }

    fn filter() -> VersionFilter {
        VersionFilter::new(
            reqwest::Client::new(),
            MinimumVersionRequirements {
                indexer_service: "1.0.0".parse().unwrap(),
                graph_node: "0.35.0".parse().unwrap(),
            },
        )
    }

    #[tokio::test]
    async fn accepts_indexer_meeting_both_minimums() {
        let url = indexer("1.0.0", graph_node_reports("0.35.0")).await;

        assert_matches!(filter().check(&url).await, Ok(()));
    }

    #[tokio::test]
    async fn rejects_indexer_whose_graph_node_does_not_report_its_version() {
        let url = indexer(
            "1.0.0",
            json!({ "errors": [{ "message": "Type `Query` has no field `version`" }] }),
        )
        .await;

        assert_matches!(
            filter().check(&url).await,
            Err(UnavailableReason::NoStatus(reason)) if reason.contains("graph-node version")
        );
    }

    #[tokio::test]
    async fn rejects_indexer_whose_graph_node_is_below_minimum() {
        let url = indexer("1.0.0", graph_node_reports("0.34.9")).await;

        assert_matches!(
            filter().check(&url).await,
            Err(UnavailableReason::NotSupported(reason)) if reason == "graph-node version below minimum"
        );
    }

    #[tokio::test]
    async fn rejects_indexer_service_below_minimum() {
        let url = indexer("0.9.0", graph_node_reports("0.35.0")).await;

        assert_matches!(
            filter().check(&url).await,
            Err(UnavailableReason::NotSupported(reason))
                if reason == "indexer-service version below minimum"
        );
    }
}
