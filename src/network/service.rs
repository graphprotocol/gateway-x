//! The [`NetworkService`] is a `graph-gateway` specific abstraction layer providing a
//! simplified interface for resolving the subgraph-specific information required by the
//! query processing pipeline

use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use ipnetwork::IpNetwork;
use semver::Version;
use thegraph_core::{
    DeploymentId, IndexerId, SubgraphId,
    alloy::primitives::{Address, BlockNumber},
};
use tokio::{
    sync::watch,
    time::{Instant, MissedTickBehavior},
};

use super::{
    DeploymentError, SubgraphError,
    cost_model::CostModelResolver,
    host_filter::HostFilter,
    indexer_blocklist,
    indexer_processing::{self, IndexerRawInfo},
    indexing_progress::IndexingProgressResolver,
    poi_filter::PoiFilter,
    pre_processing,
    snapshot::{self, Indexing, IndexingId, NetworkTopologySnapshot},
    subgraph_client::{Client as SubgraphClient, FetchResult},
    subgraph_processing::{self, DeploymentInfo, SubgraphInfo},
    version_filter::{MinimumVersionRequirements, VersionFilter},
};
use crate::errors::UnavailableReason;

/// Subgraph resolution information returned by the [`NetworkService`].
pub struct ResolvedSubgraphInfo {
    /// Subgraph chain name.
    // This is the chain name is used to retrieve the latest known block number for the chain
    // from the chain head tracking service.
    pub chain: String,
    /// Subgraph start block number.
    pub start_block: BlockNumber,
    /// The [`SubgraphId`]s associated with the query selector.
    pub subgraphs: Vec<SubgraphId>,
    /// Subgraph versions, in descending order.
    pub versions: Vec<DeploymentId>,
    /// A list of [`Indexing`]s for the resolved subgraph versions.
    pub indexings: HashMap<IndexingId, Result<Indexing, UnavailableReason>>,
}

impl ResolvedSubgraphInfo {
    /// Get the latest block number reported.
    ///
    /// The latest block number is the highest block number among all the reported progress of
    /// the indexings associated with the resolved subgraph.
    pub fn latest_reported_block(&self) -> Option<BlockNumber> {
        self.indexings
            .values()
            .filter_map(|indexing| {
                indexing
                    .as_ref()
                    .ok()
                    .map(|indexing| indexing.progress.latest_block)
            })
            .max()
    }
}

/// The [`NetworkService`] is responsible for extracting and providing information about
/// the network topology and subgraphs associated with a given query selector, e.g., a subgraph ID.
///
/// To create a new [`NetworkService`] instance, use the [`NetworkServiceBuilder`].
#[derive(Clone)]
pub struct NetworkService {
    network: watch::Receiver<NetworkTopologySnapshot>,
}

impl NetworkService {
    /// Wait for the network topology information to be available.
    pub async fn wait_until_ready(&mut self) {
        self.network
            .wait_for(|n| !n.subgraphs.is_empty() || !n.deployments.is_empty())
            .await
            .unwrap();
    }

    /// Wait for the network topology information to change.
    pub async fn changed(&mut self) {
        self.network.changed().await.unwrap();
    }

    /// Given a [`SubgraphId`], resolve the deployments associated with the subgraph.
    ///
    /// If the subgraph is not found, returns `Ok(None)`.
    pub fn resolve_with_subgraph_id(
        &self,
        id: &SubgraphId,
    ) -> Result<Option<ResolvedSubgraphInfo>, SubgraphError> {
        let network = self.network.borrow();

        // Resolve the subgraph information
        let subgraph = match network.subgraphs.get(id) {
            None => return Ok(None),
            Some(Err(err)) => return Err(err.to_owned()),
            Some(Ok(subgraph)) => subgraph,
        };

        let subgraph_chain = subgraph.chain.clone();
        let subgraph_start_block = subgraph.start_block;

        Ok(Some(ResolvedSubgraphInfo {
            chain: subgraph_chain,
            start_block: subgraph_start_block,
            subgraphs: vec![subgraph.id],
            versions: subgraph.versions.clone(),
            indexings: subgraph.indexings.clone(),
        }))
    }

    /// Given a [`DeploymentId`], resolve the deployments associated with the subgraph.
    ///
    /// If the deployment is not found, returns `Ok(None)`.
    pub fn resolve_with_deployment_id(
        &self,
        id: &DeploymentId,
    ) -> Result<Option<ResolvedSubgraphInfo>, DeploymentError> {
        let network = self.network.borrow();

        // Resolve the deployment information
        let deployment = match network.deployments.get(id) {
            None => return Ok(None),
            Some(Err(err)) => return Err(err.to_owned()),
            Some(Ok(deployment)) => deployment,
        };

        let deployment_chain = deployment.chain.clone();
        let deployment_start_block = deployment.start_block;

        let subgraphs = deployment.subgraphs.iter().copied().collect::<Vec<_>>();
        let indexings = deployment.indexings.clone();

        Ok(Some(ResolvedSubgraphInfo {
            chain: deployment_chain,
            start_block: deployment_start_block,
            subgraphs,
            versions: vec![*id],
            indexings,
        }))
    }

    /// Get the latest indexed block number reported by the indexers.
    pub fn indexing_progress(&self) -> HashMap<IndexingId, BlockNumber> {
        self.network
            .borrow()
            .deployments
            .values()
            .flat_map(|result| result.iter().flat_map(|d| &d.indexings))
            .flat_map(|(id, indexing)| indexing.iter().map(|i| (*id, i.progress.latest_block)))
            .collect()
    }
}

pub fn spawn(
    http: reqwest::Client,
    subgraph_client: SubgraphClient,
    indexer_blocklist: indexer_blocklist::Blocklist,
    min_indexer_service_version: Version,
    min_graph_node_version: Version,
    indexer_host_blocklist: HashSet<IpNetwork>,
) -> NetworkService {
    let internal_state = InternalState {
        indexer_blocklist: indexer_blocklist.indexer,
        indexer_host_filter: HostFilter::new(indexer_host_blocklist)
            .expect("failed to create host resolver"),
        indexer_version_filter: VersionFilter::new(
            http.clone(),
            MinimumVersionRequirements {
                indexer_service: min_indexer_service_version,
                graph_node: min_graph_node_version,
            },
        ),
        indexer_poi_filer: PoiFilter::new(http.clone(), indexer_blocklist.poi),
        indexing_progress_resolver: IndexingProgressResolver::new(http.clone()),
        cost_model_resolver: CostModelResolver::new(http.clone()),
    };
    let network = spawn_updater_task(subgraph_client, internal_state);

    NetworkService { network }
}

pub struct InternalState {
    pub indexer_blocklist: watch::Receiver<HashMap<Address, HashSet<DeploymentId>>>,
    pub indexer_host_filter: HostFilter,
    pub indexer_version_filter: VersionFilter,
    pub indexer_poi_filer: PoiFilter,
    pub indexing_progress_resolver: IndexingProgressResolver,
    pub cost_model_resolver: CostModelResolver,
}

/// Spawn a background task to fetch the network topology information from the graph network
/// subgraph at regular intervals
fn spawn_updater_task(
    mut subgraph_client: SubgraphClient,
    state: InternalState,
) -> watch::Receiver<NetworkTopologySnapshot> {
    let (tx, rx) = watch::channel(Default::default());

    tokio::spawn(async move {
        let mut network_info: Option<PreprocessedNetworkInfo> = None;
        let update_interval = Duration::from_secs(30);

        let mut timer = tokio::time::interval(update_interval);
        timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            timer.tick().await;

            let start_time = Instant::now();
            match fetch_and_preprocess_subgraph_info(&mut subgraph_client).await {
                Ok(info) => network_info = Some(info),
                Err(network_subgraph_update_err) => tracing::error!(%network_subgraph_update_err),
            };
            if start_time.elapsed() > (update_interval * 2) {
                tracing::warn!("subgraph processing taking too long");
            }
            let network_info = match &network_info {
                Some(info) => info,
                None => continue,
            };
            let snapshot = fetch_update(network_info, &state).await;
            tracing::info!(
                subgraphs = snapshot.subgraphs.len(),
                deployments = snapshot.deployments.len(),
                indexings = snapshot
                    .deployments
                    .values()
                    .filter_map(|d| d.as_ref().ok())
                    .map(|d| d.indexings.len())
                    .sum::<usize>(),
            );

            let _ = tx.send(snapshot);
        }
    });

    rx
}

/// Fetch the subgraphs information from the graph network subgraph and performs pre-processing
/// steps, i.e., validation and conversion into the internal representation.
///
///   1. Fetch the subgraphs and unpublished deployments from the graph network subgraph.
///   2. Validate and convert the fetched info into the internal representation.
///   3. Merge unpublished deployments into the deployments map.
///
/// If the fetch fails or both collections are empty, an error is returned.
/// An unpublished-only dataset is valid, even without any published subgraphs.
///
/// Invalid info is filtered out before converting into the internal representation.
pub async fn fetch_and_preprocess_subgraph_info(
    client: &mut SubgraphClient,
) -> anyhow::Result<PreprocessedNetworkInfo> {
    // Fetch the subgraphs and unpublished deployments from the graph network subgraph
    let data = client.fetch().await?;
    preprocess_subgraph_info(data)
}

fn preprocess_subgraph_info(data: FetchResult) -> anyhow::Result<PreprocessedNetworkInfo> {
    // Keep the previous topology on entirely empty responses, as on fetch failure.
    // Published subgraphs are not a prerequisite for discovering development deployments.
    anyhow::ensure!(
        !data.subgraphs.is_empty() || !data.unpublished_deployments.is_empty(),
        "empty subgraph response"
    );

    // Pre-process (validate and convert) the fetched subgraphs information
    let mut indexers = pre_processing::into_internal_indexers_raw_info(data.subgraphs.iter());
    let subgraphs = pre_processing::into_internal_subgraphs_raw_info(data.subgraphs.into_iter());
    let mut deployments = pre_processing::into_internal_deployments_raw_info(subgraphs.values());

    // Pre-process unpublished deployments and merge them
    let unpublished_indexers = pre_processing::into_indexers_raw_info_from_unpublished_deployments(
        data.unpublished_deployments.iter(),
    );
    let unpublished_deployments = pre_processing::into_unpublished_deployments_raw_info(
        data.unpublished_deployments.into_iter(),
    );

    for (id, unpublished_indexer) in unpublished_indexers {
        match indexers.entry(id) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(unpublished_indexer);
            }
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                for (deployment_id, indexing) in unpublished_indexer.indexings {
                    entry
                        .get_mut()
                        .indexings
                        .entry(deployment_id)
                        .or_insert(indexing);
                }
            }
        }
    }

    for (id, deployment) in unpublished_deployments {
        deployments.entry(id).or_insert(deployment);
    }

    let subgraphs = subgraph_processing::process_subgraph_info(subgraphs);
    let deployments = subgraph_processing::process_deployments_info(deployments);

    Ok(PreprocessedNetworkInfo {
        subgraphs,
        deployments,
        indexers,
    })
}

/// Fetch the network topology information from the graph network subgraph.
pub async fn fetch_update(
    network: &PreprocessedNetworkInfo,
    state: &InternalState,
) -> NetworkTopologySnapshot {
    // Process network topology information
    let indexers_info = indexer_processing::process_info(state, &network.indexers).await;
    snapshot::new_from(
        indexers_info,
        network.subgraphs.clone(),
        network.deployments.clone(),
    )
}

pub struct PreprocessedNetworkInfo {
    subgraphs: HashMap<SubgraphId, Result<SubgraphInfo, SubgraphError>>,
    deployments: HashMap<DeploymentId, Result<DeploymentInfo, DeploymentError>>,
    indexers: HashMap<IndexerId, IndexerRawInfo>,
}

#[cfg(test)]
mod tests {
    use super::{
        super::subgraph_client::types::{
            Allocation, Indexer, Manifest, Subgraph, SubgraphDeployment, SubgraphVersion,
        },
        *,
    };

    fn deployment(seed: u8) -> SubgraphDeployment {
        SubgraphDeployment {
            id: [seed; 32].into(),
            manifest: Some(Manifest {
                network: Some("mainnet".into()),
                start_block: 123,
            }),
            allocations: vec![Allocation {
                id: Address::from([seed; 20]).into(),
                allocated_tokens: 0,
                indexer: Indexer {
                    id: Address::from([1; 20]).into(),
                    url: Some("https://indexer.example/".into()),
                    staked_tokens: 100,
                },
            }],
        }
    }

    fn published(deployment: SubgraphDeployment) -> Subgraph {
        Subgraph {
            id: [3; 32].into(),
            versions: vec![SubgraphVersion {
                _version: 1,
                subgraph_deployment: deployment,
            }],
        }
    }

    #[test]
    fn discovers_unpublished_only_with_zero_token_allocation() {
        let deployment = deployment(1);
        let id = deployment.id;
        let indexer = deployment.allocations[0].indexer.id;
        let info = preprocess_subgraph_info(FetchResult {
            subgraphs: vec![],
            unpublished_deployments: vec![deployment],
        })
        .unwrap();

        assert!(info.subgraphs.is_empty());
        let found = info.deployments[&id].as_ref().unwrap();
        assert!(found.subgraphs.is_empty());
        assert_eq!(found.manifest_network, "mainnet");
        assert_eq!(found.manifest_start_block, 123);
        assert_eq!(found.allocations[0].indexer, indexer);
        assert!(info.indexers[&indexer].indexings.contains_key(&id));
    }

    #[test]
    fn merges_mixed_discovery_and_accepts_refresh_without_published_entries() {
        let old = deployment(1);
        let new = deployment(2);
        let indexer = old.allocations[0].indexer.id;
        let mixed = preprocess_subgraph_info(FetchResult {
            subgraphs: vec![published(old.clone())],
            unpublished_deployments: vec![new.clone()],
        })
        .unwrap();
        assert_eq!(mixed.subgraphs.len(), 1);
        assert_eq!(mixed.deployments.len(), 2);
        assert_eq!(mixed.indexers[&indexer].indexings.len(), 2);

        let refreshed = preprocess_subgraph_info(FetchResult {
            subgraphs: vec![],
            unpublished_deployments: vec![new.clone()],
        })
        .unwrap();
        assert!(refreshed.subgraphs.is_empty());
        assert!(!refreshed.deployments.contains_key(&old.id));
        assert!(refreshed.deployments.contains_key(&new.id));
        assert_eq!(refreshed.indexers[&indexer].indexings.len(), 1);
    }

    #[test]
    fn keeps_published_only_discovery_working() {
        let info = preprocess_subgraph_info(FetchResult {
            subgraphs: vec![published(deployment(1))],
            unpublished_deployments: vec![],
        })
        .unwrap();
        assert_eq!(info.subgraphs.len(), 1);
        assert_eq!(info.deployments.len(), 1);
        assert_eq!(info.indexers.len(), 1);
    }

    #[tokio::test]
    async fn readiness_waits_for_topology_and_accepts_unpublished_deployments() {
        let (tx, network) = watch::channel(NetworkTopologySnapshot::default());
        let mut service = NetworkService { network };
        let mut ready = tokio_test::task::spawn(service.wait_until_ready());
        tokio_test::assert_pending!(ready.poll());

        let id: DeploymentId = [1; 32].into();
        tx.send(NetworkTopologySnapshot {
            deployments: HashMap::from([(
                id,
                Ok(snapshot::Deployment {
                    chain: "mainnet".into(),
                    start_block: 123,
                    subgraphs: Default::default(),
                    indexings: Default::default(),
                }),
            )]),
            ..Default::default()
        })
        .unwrap();
        tokio_test::assert_ready!(ready.poll());
        drop(ready);

        let resolved = service.resolve_with_deployment_id(&id).unwrap().unwrap();
        assert_eq!(resolved.chain, "mainnet");
        assert_eq!(resolved.versions, vec![id]);
        assert!(resolved.subgraphs.is_empty());
    }

    #[test]
    fn entirely_empty_response_remains_an_error() {
        let result = preprocess_subgraph_info(FetchResult {
            subgraphs: vec![],
            unpublished_deployments: vec![],
        });
        assert_eq!(result.err().unwrap().to_string(), "empty subgraph response");
    }
}
