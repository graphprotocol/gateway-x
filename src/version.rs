//! The public `/version` endpoint.
//!
//! It reports the gateway's own version and the minimum software versions an indexer must run to
//! receive queries from this gateway, so indexers and operators can check compatibility without
//! reading the gateway's configuration.

use axum::{Json, routing::MethodRouter};
use semver::Version;
use serde::Serialize;

/// The `/version` response body.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct VersionInfo {
    /// The gateway's version, from its crate version.
    pub version: Version,
    /// Version requirements an indexer's software must meet to receive queries.
    pub indexer_requirements: IndexerRequirements,
}

/// Version requirements an indexer's software must meet to receive queries.
///
/// Each is the lowest version that receives queries. Versions are compared by semver precedence,
/// as the gateway's version filter compares them, so a prerelease of a later version also meets
/// the minimum: `1.3.0-rc.1` meets `1.2.3`. A semver range such as `>=1.2.3` would not say this,
/// since ranges exclude prereleases they don't name.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct IndexerRequirements {
    /// Minimum version indexer-service reports at its `/version` endpoint.
    pub min_indexer_service_version: Version,
    /// Minimum version graph-node reports through indexer-service's `/status` endpoint. An
    /// indexer whose graph-node doesn't report a version receives no queries.
    pub min_graph_node_version: Version,
}

impl VersionInfo {
    /// Describes this gateway, given the minimum versions it was configured with.
    pub fn new(min_indexer_service_version: &Version, min_graph_node_version: &Version) -> Self {
        Self {
            version: env!("CARGO_PKG_VERSION")
                .parse()
                .expect("the crate version is valid semver"),
            indexer_requirements: IndexerRequirements {
                min_indexer_service_version: min_indexer_service_version.clone(),
                min_graph_node_version: min_graph_node_version.clone(),
            },
        }
    }
}

/// A route that answers `GET` with `info` as JSON.
pub fn route(info: VersionInfo) -> MethodRouter {
    axum::routing::get(move || std::future::ready(Json(info.clone())))
}

#[cfg(test)]
mod tests {
    use axum::{
        Router,
        body::Body,
        http::{Method, Request, StatusCode},
    };
    use headers::{ContentType, HeaderMapExt};
    use http_body_util::BodyExt;
    use serde_json::json;
    use tower::ServiceExt;

    use super::*;

    fn version(s: &str) -> Version {
        s.parse().unwrap()
    }

    #[test]
    fn reports_crate_version_and_minimums_as_requirements() {
        let info = VersionInfo::new(&version("1.0.0-rc.2"), &version("0.35.1"));

        assert_eq!(
            serde_json::to_value(&info).unwrap(),
            json!({
                "version": env!("CARGO_PKG_VERSION"),
                "indexer_requirements": {
                    "min_indexer_service_version": "1.0.0-rc.2",
                    "min_graph_node_version": "0.35.1",
                },
            })
        );
    }

    #[test]
    fn served_minimum_admits_exactly_the_versions_the_filter_admits() {
        let minimum = version("1.2.3");
        let info = VersionInfo::new(&minimum, &minimum);
        let served = serde_json::to_value(&info).unwrap();

        for field in ["min_indexer_service_version", "min_graph_node_version"] {
            let served = version(served["indexer_requirements"][field].as_str().unwrap());
            for candidate in [
                "1.2.2",
                "1.2.3",
                "1.2.4",
                "1.3.0",
                "2.0.0",
                "0.9.9",
                "1.2.3-rc.1",
                "1.3.0-rc.1",
            ] {
                let candidate = version(candidate);
                // Compared as clients are told to compare: by semver precedence.
                assert_eq!(
                    candidate >= served,
                    candidate >= minimum,
                    "{field} {candidate}"
                );
            }
        }
    }

    #[tokio::test]
    async fn get_version_returns_json() {
        let info = VersionInfo::new(&version("1.0.0"), &version("0.35.0"));
        let app = Router::new().route("/version", route(info.clone()));
        let req = Request::builder()
            .method(Method::GET)
            .uri("/version")
            .body(Body::empty())
            .unwrap();

        let res = app.oneshot(req).await.expect("to be infallible");

        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            res.headers().typed_get::<ContentType>(),
            Some(ContentType::json())
        );
        let body = res.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
            serde_json::to_value(&info).unwrap()
        );
    }
}
