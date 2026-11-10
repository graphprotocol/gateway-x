use http::{
    StatusCode,
    header::{CONTENT_LENGTH, CONTENT_TYPE},
};
use reqwest::header::AUTHORIZATION;
use serde::{Deserialize, Serialize};
use thegraph_core::{
    alloy::{
        dyn_abi::Eip712Domain,
        primitives::{BlockHash, BlockNumber},
    },
    attestation::{self, Attestation},
};
use url::Url;

use crate::{
    blocks::Block,
    errors::{
        IndexerError::{self, *},
        MissingBlockError, UnavailableReason,
    },
    receipts::Receipt,
    unattestable_errors::miscategorized_unattestable,
};

#[derive(Clone, Debug)]
pub struct IndexerResponse {
    pub response_body: String,
    pub attestation: Option<Attestation>,
    pub errors: Vec<String>,
    pub indexed_block: Option<Block>,
}

#[derive(Clone)]
pub struct IndexerClient {
    pub client: reqwest::Client,
    /// Maximum allowed response size from indexers.
    /// Prevents memory exhaustion from malicious or buggy indexers.
    pub max_response_size: usize,
}

pub enum IndexerAuth<'a> {
    Paid(&'a Receipt, &'a Eip712Domain),
    Free(&'a str),
}

impl IndexerClient {
    pub async fn query_indexer(
        &self,
        deployment_url: Url,
        auth: IndexerAuth<'_>,
        query: &str,
    ) -> Result<IndexerResponse, IndexerError> {
        let (auth_key, auth_value) = match auth {
            IndexerAuth::Paid(receipt, _) => ("tap-receipt", receipt.serialize()),
            IndexerAuth::Free(token) => (AUTHORIZATION.as_str(), format!("Bearer {token}")),
        };

        let result = self
            .client
            .post(deployment_url)
            .header(CONTENT_TYPE.as_str(), "application/json")
            .header(auth_key, auth_value)
            .body(query.to_string())
            .send()
            .await;
        let response = match result {
            Ok(response) => response,
            Err(e) if e.is_timeout() => return Err(Timeout),
            Err(e) if e.is_connect() => return Err(BadResponse("failed to connect".to_string())),
            Err(e) => return Err(BadResponse(e.to_string())),
        };
        let status = response.status();
        if status != StatusCode::OK {
            if let Ok(body) = response.text().await {
                tracing::info!(status = status.as_u16(), indexer_err_response = body);
            }
            return Err(BadResponse(status.as_u16().to_string()));
        }

        let indexed_block = response
            .headers()
            .get("graph-indexed")
            .and_then(|v| v.to_str().ok());
        tracing::debug!(indexed_block = indexed_block.unwrap_or("null"));
        let indexed_block = indexed_block.and_then(parse_graph_indexed_header);

        // Fast-path rejection: check Content-Length header before reading body
        let max_response_size = self.max_response_size;
        if let Some(content_length) = response.headers().get(CONTENT_LENGTH)
            && let Ok(len) = content_length.to_str().unwrap_or("0").parse::<usize>()
            && len > max_response_size
        {
            return Err(BadResponse(format!(
                "response too large: {len} bytes (max {max_response_size})"
            )));
        }

        // Read body with size limit to prevent memory exhaustion
        let body = read_body_limited(response, max_response_size)
            .await
            .map_err(BadResponse)?;

        #[derive(Debug, Deserialize)]
        pub struct IndexerResponsePayload {
            #[serde(rename = "graphQLResponse")]
            pub graphql_response: Option<String>,
            pub attestation: Option<Attestation>,
            pub error: Option<String>,
        }
        let payload: IndexerResponsePayload =
            serde_json::from_slice(&body).map_err(|err| BadResponse(err.to_string()))?;
        if let Some(err) = payload.error {
            return Err(BadResponse(err));
        }

        let response_body = payload
            .graphql_response
            .ok_or_else(|| BadResponse("missing response".into()))?;
        let errors = response_errors(&response_body)?;
        let errors: Vec<String> = errors.into_iter().map(|err| err.message).collect();

        errors
            .iter()
            .try_for_each(|err| check_block_error(err))
            .map_err(|err| Unavailable(UnavailableReason::MissingBlock(err)))?;
        if let Some(error) = errors
            .iter()
            .find(|error| miscategorized_unattestable(error))
        {
            return Err(BadResponse(format!("unattestable response: {error}")));
        }

        if let IndexerAuth::Paid(receipt, attestation_domain) = auth {
            match &payload.attestation {
                Some(attestation) => {
                    let allocation = receipt.allocation();
                    if let Err(err) = attestation::verify(
                        attestation_domain,
                        attestation,
                        &allocation,
                        query,
                        &response_body,
                    ) {
                        return Err(BadResponse(format!("bad attestation: {err}")));
                    }
                }
                None => {
                    let message = if !errors.is_empty() {
                        format!(
                            "no attestation: {}",
                            errors
                                .iter()
                                .map(|err| err.as_str())
                                .collect::<Vec<&str>>()
                                .join("; ")
                        )
                    } else {
                        "no attestation".to_string()
                    };
                    return Err(BadResponse(message));
                }
            };
        }

        Ok(IndexerResponse {
            response_body,
            attestation: payload.attestation,
            errors,
            indexed_block,
        })
    }
}

#[derive(Deserialize, Serialize)]
struct Error {
    message: String,
}

fn response_errors(response: &str) -> Result<Vec<Error>, IndexerError> {
    #[derive(Deserialize, Serialize)]
    struct Response {
        #[serde(default)]
        #[serde(skip_serializing_if = "Vec::is_empty")]
        errors: Vec<Error>,
    }
    let mut payload: Response =
        serde_json::from_str(response).map_err(|err| BadResponse(err.to_string()))?;

    // Avoid processing oversized errors.
    for err in &mut payload.errors {
        err.message.truncate(err.message.floor_char_boundary(256));
        err.message.shrink_to_fit();
    }

    Ok(payload.errors)
}

fn parse_graph_indexed_header(header: &str) -> Option<Block> {
    #[derive(Deserialize)]
    struct GraphIndexed {
        number: BlockNumber,
        hash: BlockHash,
        timestamp: Option<u64>,
    }

    let payload: GraphIndexed = serde_json::from_str(header).ok()?;
    Some(Block {
        number: payload.number,
        hash: payload.hash,
        timestamp: payload.timestamp?,
    })
}

fn check_block_error(err: &str) -> Result<(), MissingBlockError> {
    // Older indexers may still only communicate block availability through GraphQL errors.
    if !err.contains("Failed to decode `block") {
        return Ok(());
    }
    let extract_block_number = |prefix: &str| -> Option<u64> {
        let start = err.find(prefix)? + prefix.len();
        let str = err.split_at(start).1.split_once(' ')?.0;
        str.parse::<u64>().ok()
    };
    Err(MissingBlockError {
        missing: extract_block_number("and data for block number "),
        latest: extract_block_number("has only indexed up to block number "),
    })
}

/// Read response body with size limit, streaming to avoid unbounded memory allocation.
async fn read_body_limited(
    mut response: reqwest::Response,
    max_size: usize,
) -> Result<Vec<u8>, String> {
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|e| e.to_string())? {
        if body.len() + chunk.len() > max_size {
            return Err(format!("response exceeds {max_size} byte limit"));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

#[cfg(test)]
mod tests {
    use crate::errors::MissingBlockError;

    #[test]
    fn check_block_error() {
        let tests = [
            ("", Ok(())),
            (
                "Failed to decode `block.number` value: `subgraph QmQqLJVgZLcRduoszARzRi12qGheUTWAHFf3ixMeGm2xML has only indexed up to block number 133239690 and data for block number 133239697 is therefore not yet available",
                Err(MissingBlockError {
                    missing: Some(133239697),
                    latest: Some(133239690),
                }),
            ),
            (
                "Failed to decode `block.hash` value",
                Err(MissingBlockError {
                    missing: None,
                    latest: None,
                }),
            ),
        ];
        for (input, expected) in tests {
            assert_eq!(super::check_block_error(input), expected);
        }
    }
}

#[cfg(test)]
mod error_truncation_tests {
    use super::response_errors;

    #[test]
    fn unicode_errors_are_truncated_without_splitting_characters() {
        for (message, expected) in [
            (format!("{}é", "a".repeat(255)), "a".repeat(255)),
            (format!("{}€", "a".repeat(254)), "a".repeat(254)),
            (format!("{}🦀", "a".repeat(253)), "a".repeat(253)),
            (
                format!("{}é!", "a".repeat(254)),
                format!("{}é", "a".repeat(254)),
            ),
            ("a".repeat(300), "a".repeat(256)),
            ("short 🦀".into(), "short 🦀".into()),
        ] {
            let response = serde_json::json!({"errors": [{"message": message}]}).to_string();
            let errors = response_errors(&response).unwrap();
            assert_eq!(errors.len(), 1);
            assert_eq!(errors[0].message, expected);
        }
    }
}
