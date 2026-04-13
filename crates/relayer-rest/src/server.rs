use std::{
    error::Error,
    net::{SocketAddr, ToSocketAddrs},
};

use axum::{
    extract::{Path, Query},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Extension, Json, Router, Server,
};
use crossbeam_channel as channel;
use ibc_relayer_types::core::ics24_host::identifier::ChainId;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;

use ibc_relayer::rest::{request::Request, RestApiError};

use crate::handle::{
    all_chain_ids, assemble_version_info, chain_config, get_balances, get_history, get_pending,
    get_stats, supervisor_state, trigger_clear_packets,
};

pub type BoxError = Box<dyn Error + Send + Sync>;

pub fn spawn(
    addr: impl ToSocketAddrs,
    sender: channel::Sender<Request>,
) -> Result<JoinHandle<()>, BoxError> {
    let addr = addr.to_socket_addrs()?.next().unwrap();
    let handle = tokio::spawn(run(addr, sender));
    Ok(handle)
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "status", content = "result")]
#[serde(rename_all = "lowercase")]
enum JsonResult<R, E> {
    Success(R),
    Error(E),
}

impl<R, E> From<Result<R, E>> for JsonResult<R, E> {
    fn from(r: Result<R, E>) -> Self {
        match r {
            Ok(a) => Self::Success(a),
            Err(e) => Self::Error(e),
        }
    }
}

/// Wraps a Result into a JSON response, returning HTTP 503 if the error is a supervisor timeout.
fn json_response<R: Serialize>(result: Result<R, RestApiError>) -> Response {
    let is_timeout = matches!(&result, Err(RestApiError::SupervisorTimeout));
    let json_result = JsonResult::from(result);
    let json = Json(json_result);

    if is_timeout {
        (StatusCode::SERVICE_UNAVAILABLE, json).into_response()
    } else {
        json.into_response()
    }
}

async fn get_version(Extension(sender): Extension<Sender>) -> Response {
    let version: Result<_, RestApiError> = Ok(assemble_version_info(&sender));
    json_response(version)
}

async fn get_chains(Extension(sender): Extension<Sender>) -> Response {
    json_response(all_chain_ids(&sender))
}

async fn get_chain(
    Path(id): Path<String>,
    Extension(sender): Extension<Sender>,
) -> Response {
    json_response(chain_config(&sender, &id))
}

async fn get_state(Extension(sender): Extension<Sender>) -> Response {
    json_response(supervisor_state(&sender))
}

#[derive(Debug, Deserialize)]
struct ClearPacketParams {
    chain: Option<ChainId>,
}

async fn clear_packets(
    Extension(sender): Extension<Sender>,
    Query(params): Query<ClearPacketParams>,
) -> Response {
    json_response(trigger_clear_packets(&sender, params.chain))
}

#[derive(Debug, Deserialize)]
struct HistoryParams {
    #[serde(default = "default_limit")]
    limit: usize,
    chain: Option<String>,
}

fn default_limit() -> usize {
    100
}

async fn history(
    Extension(sender): Extension<Sender>,
    Query(params): Query<HistoryParams>,
) -> Response {
    json_response(get_history(&sender, params.limit, params.chain))
}

async fn stats(Extension(sender): Extension<Sender>) -> Response {
    json_response(get_stats(&sender))
}

#[derive(Debug, Deserialize)]
struct PendingParams {
    chain: Option<ChainId>,
}

async fn pending(
    Extension(sender): Extension<Sender>,
    Query(params): Query<PendingParams>,
) -> Response {
    json_response(get_pending(&sender, params.chain))
}

async fn balances(Extension(sender): Extension<Sender>) -> Response {
    json_response(get_balances(&sender))
}

type Sender = channel::Sender<Request>;

async fn run(addr: SocketAddr, sender: Sender) {
    let app = Router::new()
        .route("/version", get(get_version))
        .route("/chains", get(get_chains))
        .route("/chain/:id", get(get_chain))
        .route("/state", get(get_state))
        .route("/clear_packets", post(clear_packets))
        .route("/history", get(history))
        .route("/stats", get(stats))
        .route("/pending", get(pending))
        .route("/balances", get(balances))
        .layer(Extension(sender));

    Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}
