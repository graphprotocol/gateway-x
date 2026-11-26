# gateway

## overview

A gateway, in the Graph Network, is a client capable of managing relationships between many
data consumers and many indexers. A gateway is not necessary for a consumer to access the indexers
participating in the Graph Network, though it does simplify consumer interactions.

A gateway is expected to be a reliable system, to compensate for indexers being relatively
unreliable. Indexers may become unresponsive, malicious, or otherwise unsuitable for serving queries
at any time without warning. It is the responsibility of the gateway to maintain the highest
possible quality of service (QoS) to clients under these conditions.

The gateway's primary responsibilities are to serve client requests and to facilitate indexer
payments. Other responsibilities, though important, are secondary and therefore their failure modes
must have minimal impact on the primary responsibilities.

## indexer discovery

For the gateway to route client queries to indexers, it must be able to associate subgraph and
subgraph deployment IDs with the indexers that have active allocations on the subgraph deployment
being queried. The tree of subgraphs, subgraph deployments (versions), and allocated indexers is
accessible via the network subgraph which indexes the Graph Network contracts.

The gateway periodically queries the network subgraph for this data using a set of trusted indexers.
The trusted indexers are not necessary theoretically, but they avoid an otherwise cumbersome
bootstrapping process for payments.

When an indexer registers itself via the contract, it provides a URL to access its indexer-service.
After the subgraph data is collected and organized, the gateway requests more information from each
active indexer via the indexer-service. This includes software version information and, for each
allocation, the indexing status (progress on chain indexed by subgraph deployment) and cost models.

Indexers running an indexer-service older than `min_indexer_version`, or a graph-node older than
`min_graph_node_version`, are not sent queries. The gateway serves these requirements, with its own
version, at `GET /version`:

```json
{
  "version": "27.7.1",
  "indexer_requirements": {
    "min_indexer_service_version": "1.0.0",
    "min_graph_node_version": "0.35.0"
  }
}
```

Versions are compared by semver precedence, so a prerelease of a later version meets a minimum
(`1.3.0-rc.1` meets `1.2.3`). An indexer whose graph-node doesn't report its version receives
no queries.

The gateway may be configured to block public Proofs Of Indexing (POIs) that have been associated
with bad query responses. In this case deployments with such POIs require an additional step in
discovery where indexers are required to submit their public POI. If the indexer's POI has been
blocked, the indexer will not be considered to serve queries on the associated deployment until it
returns a good POI in a subsequent request.

## auth

The gateway requires client requests (queries) to include an API key which associates the request
with some consumer to track usage for payment to the gateway operator. The API key may have
additional settings or restrictions that are checked before executing or rejecting the request.

## queries

Request paths can take 3 general shapes:

- subgraph ID (from GNS contract)
- deployment ID (IPFS hash of manifest)
- deployment ID & indexer address

Requests by subgraph ID must first be resolved to some deployment ID. The gateway selects the latest
deployment where some indexer reports an indexing status within 30 minutes of chain head. If no
deployment meets this requirement, the latest deployment is selected.

Requests specifying an indexer address are only intended to facilitate cross-checking indexer
responses. Using this option for production data requests are not guaranteed to behave as expected.
The rest of this section will assume that an indexer address has not been provided in the request.

The indexer request may be rewritten by the gateway to get additional data required by the gateway
to track the progress of each indexer relative to the indexed chain. The request containing the
client request and potentially additional data is called the "indexer request".

A subset of up to 3 indexers will be selected to execute the indexer request. These indexers are
selected based on some combination of the following criteria (implementation at https://github.com/edgeandnode/candidate-selection):

- success rate
- expected latency
- seconds behind chain head
- slashable GRT
- fee requested from indexer cost model (relative to gateway budget)

The first response from an indexer (that passes through some additional filters) is returned to the
client, after stripping out data not requested by the client. All indexer responses are used to feed
back performance information into the indexer selection algorithm. If all selected indexers fail to
respond to the request, then this process is repeated until all available indexers are exhausted.

## data science

The gateway exports data into the following kafka topics:

- queries (`gateway_queries`)
- attestations (`gateway_attestations`)

The gateway also consumes from `gateway_blocklist`.

When multiple gateway environments share a single Kafka cluster, set `kafka_topic_environment` in the
config to append an environment qualifier to all topic names. For example, setting
`"kafka_topic_environment": "staging"` produces topics like `gateway_queries_staging`. When unset,
the default topic names above are used.

Optionally, the [titorelli](https://github.com/edgeandnode/titorelli/) system can do aggregations
over these topics. For now, this is limited to creating `gateway_indexer_fees_hourly` to improve
the startup time of the `tap-escrow-manager`.

## indexer payments

Indexers make their prices available via Agora cost-models. These cost models are served, for each
subgraph deployment, by indexer-service at `/cost`. When selecting indexers, the gateway first
executes their cost models over the indexer request to obtain each indexer's fee. Indexer selection
will favor indexers with lower fees, all else being equal. The gateway has a control system that may
pay indexers more than they request via their cost models in an effort to hit an average fees per
client query. Indexer fees are clamped to a maximum of the gateway's budget.

### TAP

For an overview of TAP see https://github.com/semiotic-ai/timeline-aggregation-protocol.

This is a horizon-ready gateway that generates TAP v2 receipts exclusively. The gateway acts as a TAP sender, where each indexer request is sent with a TAP v2 receipt. The gateway
operator is expected to run 2 additional services:

- [tap-aggregator](https://github.com/semiotic-ai/timeline-aggregation-protocol/tree/main/tap_aggregator):
  public endpoint where indexers can aggregate receipts into RAVs
- [tap-escrow-manager](https://github.com/edgeandnode/tap-escrow-manager):
  maintains escrow balances for the TAP sender. This service requires data exported by the gateway
  into the "indexer requests" topic to calculate the value of outstanding receipts to each indexer.

The gateway operator is also expected to manage at least 2 wallets:

- sender: requires ETH for transaction gas and GRT to allocate into TAP escrow balances for paying indexers
- authorized signer: used by the gateway and tap-aggregator to sign v2 receipts and RAVs

## operational notes

### configuration

Nearly all configuration is done via a single JSON configuration file, the path of which must be
given as the first argument to the graph-gateway executable.
e.g. `graph-gateway path/to/config.json`. The structure of the configuration file is defined in
[config.rs](src/config.rs) (`graph_gateway::config::Config`).

Log filtering is set using the `RUST_LOG` environment variable. For example, if you would like to
set the default log level to `info`, but want to set the log level for the `graph_gateway` module to
`debug`, you would use `RUST_LOG="info,graph_gateway=debug"`. More details on environment variable
filtering: https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html.

### errors

Each error returned to the client when making a request is defined in [errors.rs](src/errors.rs) (`gateway_framework::errors::Error`).

### logs

Log events are emitted for each client request and all of its associated indexer requests. These can
be found using the span label `client_request`. The indexer request log events also contain the
label `indexer_request`.

log levels:

- `error`: An unexpected state has been reached, which is likely to have a negative impact on the
  gateway's ability to serve queries or make payments.
- `warn`: An unexpected state has been reached, though it is recoverable and unlikely to have a
  negative impact on the gateway's ability to serve queries or make payments.
- `info`: Information that is commonly used to trace the execution of the major gateway subsystems
  in production.
- `debug`: Similar to `info`, but is often irrelevant when investigating gateway execution in
  production.
- `trace`: Information that is considered too verbose for production, but is often useful during
  development.

### metrics

Prometheus metrics are served at `:${METRICS_PORT}/metrics`.
The available metrics are defined in [metrics.rs](src/metrics.rs).
