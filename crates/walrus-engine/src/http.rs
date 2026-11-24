//! HTTP endpoints (PLAN.md §6.1, §7.5): `/v1/stream`, `/health`, `/ready`, `/metrics`.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use anyhow::{Context, Result};
use axum::Router;
use axum::body::Body;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use futures_util::StreamExt;
use serde::Deserialize;
use tokio::net::TcpListener;
use tokio_stream::wrappers::ReceiverStream;
use walrus_protocol::Offset;

use crate::metrics::Metrics;
use crate::schema::TableSchema;
use crate::session::{self, Request, Shared};

/// Serve until `shared.shutdown` becomes true.
///
/// `GET /v1/stream?tables=a.b,c.d[&offset=<offset>]`, header `Last-Event-ID: <offset>`:
/// - the header wins over the `offset` query parameter (browsers send the header on automatic
///   reconnects; the query parameter is how a persisted client resumes on a fresh page load);
/// - 503 + `Retry-After: 1` until `shared.ready` is true;
/// - 400 for an empty or unknown table list or a malformed offset;
/// - otherwise 200 with `Content-Type: text/event-stream`, `Cache-Control: no-store`,
///   `X-Accel-Buffering: no`, and the body streamed from `session::spawn`;
///   `metrics.sessions_active` counts open bodies.
///
/// `/health` → 200 "ok"; `/ready` → 200 or 503; `/metrics` → `Metrics::render()` as
/// `text/plain; version=0.0.4`.
pub async fn serve(listener: TcpListener, shared: Arc<Shared>) -> Result<()> {
    let mut shutdown = shared.shutdown.clone();
    let app = Router::new()
        .route("/v1/stream", get(stream))
        .route("/health", get(|| async { "ok" }))
        .route("/ready", get(ready))
        .route("/metrics", get(metrics))
        .with_state(shared);
    // Sessions also end on shutdown, so the open streams do not hold the server up.
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = shutdown.wait_for(|s| *s).await;
        })
        .await
        .context("http server failed")
}

#[derive(Deserialize)]
struct StreamQuery {
    tables: Option<String>,
    offset: Option<String>,
}

/// Validate the request parameters (pure). `Err` is the short 400 body.
pub fn parse_request(
    schemas: &[TableSchema],
    tables_param: Option<&str>,
    header_offset: Option<&str>,
    query_offset: Option<&str>,
) -> Result<Request, String> {
    let list = tables_param
        .filter(|s| !s.is_empty())
        .ok_or("missing `tables` parameter")?;
    let mut tables = Vec::new();
    for name in list.split(',').map(str::trim) {
        let id = schemas
            .iter()
            .position(|t| t.name == name)
            .ok_or_else(|| format!("unknown table `{name}`"))?;
        if !tables.contains(&id) {
            tables.push(id);
        }
    }
    // The header wins: it is what the browser sends on its own reconnects. Empty means absent.
    let offset = header_offset
        .filter(|s| !s.is_empty())
        .or(query_offset.filter(|s| !s.is_empty()))
        .map(|s| s.parse::<Offset>().map_err(|_| "malformed offset"))
        .transpose()?;
    Ok(Request { tables, offset })
}

async fn stream(
    State(shared): State<Arc<Shared>>,
    Query(query): Query<StreamQuery>,
    headers: HeaderMap,
) -> Response {
    if !*shared.ready.borrow() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            [(header::RETRY_AFTER, "1")],
            "not ready",
        )
            .into_response();
    }
    let header_offset = match headers.get("last-event-id").map(|v| v.to_str()) {
        None => None,
        Some(Ok(s)) => Some(s),
        Some(Err(_)) => return (StatusCode::BAD_REQUEST, "malformed offset").into_response(),
    };
    let request = match parse_request(
        &shared.tables,
        query.tables.as_deref(),
        header_offset,
        query.offset.as_deref(),
    ) {
        Ok(r) => r,
        Err(msg) => return (StatusCode::BAD_REQUEST, msg).into_response(),
    };

    let guard = SessionGuard::new(shared.metrics.clone());
    let rx = session::spawn(shared, request);
    // The guard lives as long as the body stream, so `sessions_active` counts open bodies.
    let body = Body::from_stream(ReceiverStream::new(rx).map(move |frame| {
        let _open = &guard;
        Ok::<_, std::convert::Infallible>(frame)
    }));
    (
        [
            (header::CONTENT_TYPE, "text/event-stream"),
            (header::CACHE_CONTROL, "no-store"),
            (header::HeaderName::from_static("x-accel-buffering"), "no"),
        ],
        body,
    )
        .into_response()
}

/// Counts an open session body in `sessions_active`; decrements on drop.
struct SessionGuard {
    metrics: Arc<Metrics>,
}

impl SessionGuard {
    fn new(metrics: Arc<Metrics>) -> Self {
        Metrics::inc(&metrics.sessions_active);
        SessionGuard { metrics }
    }
}

impl Drop for SessionGuard {
    fn drop(&mut self) {
        let counter: &AtomicU64 = &self.metrics.sessions_active;
        counter.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
}

async fn ready(State(shared): State<Arc<Shared>>) -> Response {
    if *shared.ready.borrow() {
        (StatusCode::OK, "ready").into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready").into_response()
    }
}

async fn metrics(State(shared): State<Arc<Shared>>) -> Response {
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        shared.metrics.render(),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ColumnSchema;

    fn schemas() -> Vec<TableSchema> {
        ["public.todos", "public.lists"]
            .iter()
            .map(|n| TableSchema {
                name: n.to_string(),
                columns: vec![ColumnSchema {
                    name: "id".into(),
                    type_name: "bigint".into(),
                    type_oid: 20,
                }],
                key: vec![0],
            })
            .collect()
    }

    const OFF_A: &str = "9f2c4e1a7b3d5f60-00000000016b3740";
    const OFF_B: &str = "0000000000000001-0000000000000002";

    #[test]
    fn parses_and_dedups_tables() {
        let r = parse_request(
            &schemas(),
            Some("public.lists,public.todos,public.lists"),
            None,
            None,
        )
        .expect("valid");
        assert_eq!(r.tables, vec![1, 0]);
        assert!(r.offset.is_none());
    }

    #[test]
    fn rejects_bad_tables() {
        for p in [None, Some(""), Some("public.nope"), Some("public.todos,")] {
            assert!(parse_request(&schemas(), p, None, None).is_err(), "{p:?}");
        }
    }

    #[test]
    fn header_wins_over_query() {
        let r = parse_request(&schemas(), Some("public.todos"), Some(OFF_A), Some(OFF_B))
            .expect("valid");
        assert_eq!(r.offset.expect("offset").to_string(), OFF_A);
        let r = parse_request(&schemas(), Some("public.todos"), None, Some(OFF_B)).expect("valid");
        assert_eq!(r.offset.expect("offset").to_string(), OFF_B);
        let r = parse_request(&schemas(), Some("public.todos"), Some(""), None).expect("valid");
        assert!(r.offset.is_none());
    }

    #[test]
    fn malformed_offset_is_an_error() {
        let e = parse_request(&schemas(), Some("public.todos"), Some("nope"), Some(OFF_A))
            .expect_err("header is malformed");
        assert_eq!(e, "malformed offset");
        assert!(parse_request(&schemas(), Some("public.todos"), None, Some("1-2")).is_err());
    }
}
