//! The start-up memory probe must not hold the accept loop (#795): the
//! server is bound at once, and a client is served while the probe
//! still runs. The probe once sat inline in the serve task, and its
//! read of the host's memory lasted as long as a client that had
//! connected through the backlog and was waiting for the server - so
//! the client's own keep-alive gave up first.

#![cfg(unix)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use scion_proto::runtime::listen::Limits;
use scion_server::floor::MEMSIZE_PROBE;
use scion_server::test_support::service;
use scion_server::{Backend, Quotas};
use tonic::transport::Channel;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;

const TOKEN: &str = "secret";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_server_serves_while_the_startup_memory_probe_blocks() {
    // The probe blocks well past the assertion's second: without the
    // fix it runs inline in the serve task, and the health call below
    // cannot be served inside it. The gauges may name any host: only
    // the wait matters here.
    MEMSIZE_PROBE.lock().unwrap().replace(|| {
        std::thread::sleep(Duration::from_secs(2));
        Some(64 << 30)
    });
    let dir = tempfile::tempdir().unwrap();
    let store = Backend::open_test(dir.path()).expect("the store opens");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(service::serve(
        store,
        listener,
        TOKEN,
        service::ServeOptions {
            data: dir.path().to_path_buf(),
            audit: scion_server::test_support::audit::Audit::off(),
            limits: Limits::default(),
            quotas: Quotas::off(),
            pending_lands: None,
            land_stale_secs: 1 << 40,
            tls: None,
            metrics: Arc::default(),
            reply_memory_budget: scion_server::REPLY_MEMORY_BUDGET,
        },
        None,
        std::future::pending(),
    ));

    // From before the first connection to the health answer: a second,
    // while the probe still sleeps. The connect itself completes from
    // the kernel's backlog whatever the server does, so the proof is
    // the call's answer, not the socket.
    let started = Instant::now();
    let served = tokio::time::timeout(Duration::from_secs(1), async move {
        let channel = Channel::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect()
            .await
            .expect("the server listens");
        let reply = HealthClient::new(channel)
            .check(tonic::Request::new(HealthCheckRequest {
                service: String::new(),
            }))
            .await
            .expect("the health service answers")
            .into_inner();
        ServingStatus::try_from(reply.status).expect("a known status")
    })
    .await;
    MEMSIZE_PROBE.lock().unwrap().take();
    let status = served.expect("the server serves inside the probe's wait");
    assert_eq!(status, ServingStatus::Serving);
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "{:?}",
        started.elapsed()
    );
}
