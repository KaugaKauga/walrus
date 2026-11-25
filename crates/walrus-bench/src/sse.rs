//! A lean SSE reader for benchmarks. Unlike `walrus-client` it keeps no state and clones
//! nothing per commit, so it adds as little as possible to what we measure.

use std::net::SocketAddr;

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use futures_util::StreamExt;
use futures_util::stream::BoxStream;

pub struct Event {
    /// Empty for comments and `retry:` lines.
    pub name: String,
    pub data: String,
}

pub struct Sse {
    stream: BoxStream<'static, reqwest::Result<Bytes>>,
    buf: Vec<u8>,
    /// Where to resume looking for the blank line, so big events are not rescanned per chunk.
    scan_from: usize,
    /// Bytes of all events returned so far (what a client has consumed, not what is buffered).
    pub bytes: u64,
}

impl Sse {
    pub async fn open(addr: SocketAddr, tables: &str, last_event_id: Option<&str>) -> Result<Sse> {
        let mut req =
            reqwest::Client::new().get(format!("http://{addr}/v1/stream?tables={tables}"));
        if let Some(id) = last_event_id {
            req = req.header("Last-Event-ID", id);
        }
        let resp = req.send().await.context("stream request")?;
        if !resp.status().is_success() {
            bail!("stream request failed: {}", resp.status());
        }
        Ok(Sse {
            stream: resp.bytes_stream().boxed(),
            buf: Vec::new(),
            scan_from: 0,
            bytes: 0,
        })
    }

    /// The next event, or `None` when the server closed the stream.
    pub async fn next(&mut self) -> Result<Option<Event>> {
        loop {
            let found = self.buf[self.scan_from..]
                .windows(2)
                .position(|w| w == b"\n\n");
            if let Some(pos) = found {
                let end = self.scan_from + pos;
                let event = parse(&self.buf[..end])?;
                self.bytes += end as u64 + 2;
                self.buf.drain(..end + 2);
                self.scan_from = 0;
                return Ok(Some(event));
            }
            self.scan_from = self.buf.len().saturating_sub(1);
            match self.stream.next().await {
                Some(chunk) => self.buf.extend_from_slice(&chunk.context("stream read")?),
                None => return Ok(None),
            }
        }
    }
}

fn parse(raw: &[u8]) -> Result<Event> {
    let text = std::str::from_utf8(raw).context("event is not utf-8")?;
    let (mut name, mut data) = (String::new(), String::new());
    for line in text.lines() {
        if let Some(v) = line.strip_prefix("event:") {
            name = v.trim_start().to_string();
        } else if let Some(v) = line.strip_prefix("data:") {
            if !data.is_empty() {
                data.push('\n');
            }
            data.push_str(v.strip_prefix(' ').unwrap_or(v));
        }
    }
    Ok(Event { name, data })
}
