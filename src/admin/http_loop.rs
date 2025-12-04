//! Minimal single-thread HTTP loop for stateless compaction triggers.
//! This avoids Send/Sync requirements by running on a current-thread Tokio runtime.

use std::{io, net::SocketAddr, sync::Arc};

use http::StatusCode;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::LocalSet,
};

use crate::{
    admin::server::{CompactionHttpState, TriggerPayload, handle_trigger},
    compaction::{executor::CompactionExecutor, planner::CompactionPlanner},
    mode::Mode,
};

/// Start a single-thread HTTP listener that accepts POST /compaction/trigger with JSON payload.
pub async fn serve_single_thread<M, E, CE, P>(
    addr: SocketAddr,
    state: CompactionHttpState<M, E, CE, P>,
) -> io::Result<()>
where
    M: Mode + 'static,
    M::Key: Eq + Clone,
    E: fusio::executor::Executor + fusio::executor::Timer + 'static,
    CE: CompactionExecutor + 'static,
    P: CompactionPlanner + 'static,
{
    let listener = TcpListener::bind(addr).await?;
    let state = Arc::new(state);
    let local = LocalSet::new();
    local
        .run_until(async move {
            loop {
                let (socket, _) = listener.accept().await?;
                let state = Arc::clone(&state);
                tokio::task::spawn_local(handle_conn(socket, state));
            }
        })
        .await
}

async fn handle_conn<M, E, CE, P>(
    mut socket: TcpStream,
    state: Arc<CompactionHttpState<M, E, CE, P>>,
) where
    M: Mode,
    M::Key: Eq + Clone,
    E: fusio::executor::Executor + fusio::executor::Timer,
    CE: CompactionExecutor,
    P: CompactionPlanner,
{
    let mut buf = Vec::new();
    let (status, body) = match read_request(&mut socket, &mut buf).await {
        Ok(()) => match parse_and_handle(&buf, &state).await {
            Ok(status) => (status, format!("{}\r\n", status.as_str())),
            Err(_) => (StatusCode::BAD_REQUEST, "bad request\r\n".to_string()),
        },
        Err(_) => (StatusCode::BAD_REQUEST, "bad request\r\n".to_string()),
    };
    let response = format!(
        "HTTP/1.1 {}\r\nContent-Length: {}\r\n\r\n{}",
        status.as_str(),
        body.len(),
        body
    );
    let _ = socket.write_all(response.as_bytes()).await;
    let _ = socket.shutdown().await;
}

async fn read_request(socket: &mut TcpStream, buf: &mut Vec<u8>) -> Result<(), ()> {
    const MAX_BYTES: usize = 1024 * 1024;
    let mut tmp = [0u8; 4096];
    let mut content_len: Option<usize> = None;
    loop {
        let n = socket.read(&mut tmp).await.map_err(|_| ())?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
        if buf.len() > MAX_BYTES {
            return Err(());
        }
        if content_len.is_none() {
            if let Some(headers_end) = find_headers_end(buf) {
                content_len = parse_content_length(&buf[..headers_end]);
                if content_len.unwrap_or(0) == 0 {
                    return Ok(());
                }
                if buf.len() >= headers_end + content_len.unwrap() {
                    return Ok(());
                }
            }
        } else if let Some(len) = content_len {
            if let Some(headers_end) = find_headers_end(buf) {
                if buf.len() >= headers_end + len {
                    return Ok(());
                }
            }
        }
    }
    Ok(())
}

fn find_headers_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|idx| idx + 4)
        .or_else(|| buf.windows(2).position(|w| w == b"\n\n").map(|idx| idx + 2))
}

fn parse_content_length(headers: &[u8]) -> Option<usize> {
    let text = std::str::from_utf8(headers).ok()?;
    for line in text.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("Content-Length:") {
            return rest.trim().parse().ok();
        }
    }
    None
}

async fn parse_and_handle<M, E, CE, P>(
    buf: &[u8],
    state: &CompactionHttpState<M, E, CE, P>,
) -> Result<StatusCode, ()>
where
    M: Mode,
    M::Key: Eq + Clone,
    E: fusio::executor::Executor + fusio::executor::Timer,
    CE: CompactionExecutor,
    P: CompactionPlanner,
{
    let req = String::from_utf8_lossy(buf);
    let mut lines = req.lines();
    let first = lines.next().ok_or(())?;
    if !first.starts_with("POST /compaction/trigger") {
        return Err(());
    }
    // naive content-length parse
    let mut content_len = None;
    for line in &mut lines {
        let line = line.trim();
        if line.is_empty() {
            break;
        }
        if let Some(rest) = line.strip_prefix("Content-Length:") {
            content_len = rest.trim().parse::<usize>().ok();
        }
    }
    let headers_end = find_headers_end(buf).ok_or(())?;
    let body = &buf[headers_end..];
    if let Some(len) = content_len {
        if body.len() < len {
            return Err(());
        }
    }
    let payload: TriggerPayload = serde_json::from_slice(body).map_err(|_| ())?;
    Ok(handle_trigger(state, payload).await)
}
