//! The public `/version` endpoint.
//!
//! It reports the gateway's own version and the minimum software versions an indexer must run to
//! receive queries from this gateway, so indexers and operators can check compatibility without
//! reading the gateway's configuration.

use axum::{Json, routing::MethodRouter};
use semver::{Comparator, Op, Version, VersionReq};
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
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct IndexerRequirements {
    /// Requirement on the version indexer-service reports at its `/version` endpoint.
    pub indexer_service: VersionReq,
    /// Requirement on the version graph-node reports through indexer-service's `/status`
    /// endpoint. An indexer whose graph-node doesn't report a version is not excluded.
    pub graph_node: VersionReq,
}

impl VersionInfo {
    /// Describes this gateway, given the minimum versions it was configured with.
    pub fn new(min_indexer_service_version: &Version, min_graph_node_version: &Version) -> Self {
        Self {
            version: env!("CARGO_PKG_VERSION")
                .parse()
                .expect("the crate version is valid semver"),
            indexer_requirements: IndexerRequirements {
                indexer_service: at_least(min_indexer_service_version),
                graph_node: at_least(min_graph_node_version),
            },
        }
    }
}

/// A route that answers `GET` with `info` as JSON.
pub fn route(info: VersionInfo) -> MethodRouter {
    axum::routing::get(move || std::future::ready(Json(info.clone())))
}

/// The requirement `>=minimum`, which matches the gateway's `version < minimum` exclusion.
fn at_least(minimum: &Version) -> VersionReq {
    VersionReq {
        comparators: vec![Comparator {
            op: Op::GreaterEq,
            major: minimum.major,
            minor: Some(minimum.minor),
            patch: Some(minimum.patch),
            pre: minimum.pre.clone(),
        }],
    }
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
                    "indexer_service": ">=1.0.0-rc.2",
                    "graph_node": ">=0.35.1",
                },
            })
        );
    }

    #[test]
    fn requirement_admits_exactly_the_versions_the_filter_admits() {
        let minimum = version("1.2.3");
        let requirement = at_least(&minimum);

        for candidate in ["1.2.2", "1.2.3", "1.2.4", "1.3.0", "2.0.0", "0.9.9"] {
            let candidate = version(candidate);
            assert_eq!(
                requirement.matches(&candidate),
                candidate >= minimum,
                "{candidate}"
            );
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
