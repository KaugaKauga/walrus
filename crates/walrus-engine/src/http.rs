//! HTTP endpoints (PLAN.md §6.1, §7.5): `/v1/stream`, `/health`, `/ready`, `/metrics`.

use std::sync::Arc;

use anyhow::Result;
use tokio::net::TcpListener;

use crate::session::Shared;

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
    let _ = (listener, shared);
    todo!()
}
