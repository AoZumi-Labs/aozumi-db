//! aozumi-db: a database per app, served over libSQL's HTTP protocol.
//! See DESIGN.md for the decisions behind this file.

mod exec;
mod proto;
mod registry;

use std::convert::Infallible;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

use proto::{PipelineReq, PipelineResp, StreamRequest, StreamResponse, StreamResult};
use registry::Registry;

const MAX_BODY_BYTES: usize = 10 * 1024 * 1024;
const DEV_DATABASE: &str = "default";

pub struct Config {
    pub data_dir: PathBuf,
    /// HMAC key for tokens and the admin API. `None` runs in dev mode.
    pub secret: Option<Vec<u8>>,
}

struct State {
    registry: Registry,
}

pub async fn serve(listener: TcpListener, config: Config) -> anyhow::Result<()> {
    let state = Arc::new(State { registry: Registry::new(config.data_dir) });
    loop {
        let (stream, _) = listener.accept().await?;
        let state = state.clone();
        tokio::spawn(async move {
            let service = service_fn(move |req| {
                let state = state.clone();
                async move { Ok::<_, Infallible>(handle(state, req).await) }
            });
            // A client that hangs up mid-request is not a server error.
            let _ = http1::Builder::new().serve_connection(TokioIo::new(stream), service).await;
        });
    }
}

type Resp = Response<Full<Bytes>>;

async fn handle(state: Arc<State>, req: Request<Incoming>) -> Resp {
    match (req.method(), req.uri().path()) {
        (&Method::GET, "/health") => json(StatusCode::OK, &"ok"),
        (&Method::GET, "/v2") => json(StatusCode::OK, &serde_json::json!({})),
        (&Method::POST, "/v2/pipeline") => pipeline(&state, req).await,
        _ => text(StatusCode::NOT_FOUND, "not found"),
    }
}

async fn pipeline(state: &State, req: Request<Incoming>) -> Resp {
    let body = match Limited::new(req.into_body(), MAX_BODY_BYTES).collect().await {
        Ok(body) => body.to_bytes(),
        Err(_) => return text(StatusCode::PAYLOAD_TOO_LARGE, "request body too large"),
    };
    let msg: PipelineReq = match serde_json::from_slice(&body) {
        Ok(msg) => msg,
        Err(err) => return text(StatusCode::BAD_REQUEST, &format!("invalid pipeline request: {err}")),
    };

    let db = match state.registry.open(DEV_DATABASE).await {
        Ok(db) => db,
        Err(err) => return text(StatusCode::INTERNAL_SERVER_ERROR, &format!("cannot open database: {err}")),
    };
    let conn = match db.connect() {
        Ok(conn) => conn,
        Err(err) => return text(StatusCode::INTERNAL_SERVER_ERROR, &format!("cannot connect: {err}")),
    };

    let mut results = Vec::with_capacity(msg.requests.len());
    for request in &msg.requests {
        let outcome = match request {
            StreamRequest::Close => Ok(StreamResponse::Close),
            StreamRequest::Execute { stmt } => match &stmt.sql {
                Some(sql) => exec::execute(&conn, sql, stmt).await.map(|result| StreamResponse::Execute { result }),
                None => Err(proto::Error::new("statement has no sql", "SQL_NOT_FOUND")),
            },
            _ => Err(proto::Error::new("request type not supported yet", "NOT_SUPPORTED")),
        };
        results.push(match outcome {
            Ok(response) => StreamResult::Ok { response },
            Err(error) => StreamResult::Error { error },
        });
    }

    json(StatusCode::OK, &PipelineResp { baton: None, base_url: None, results })
}

fn json<T: serde::Serialize>(status: StatusCode, body: &T) -> Resp {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(serde_json::to_vec(body).unwrap())))
        .unwrap()
}

fn text(status: StatusCode, body: &str) -> Resp {
    Response::builder()
        .status(status)
        .header("content-type", "text/plain; charset=utf-8")
        .body(Full::new(Bytes::from(body.to_string())))
        .unwrap()
}
