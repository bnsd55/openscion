//! The whole Postgres connect is bounded: the socket connect, its TLS,
//! the startup message and the authentication. tokio-postgres's
//! `connect_timeout` covers only the socket connect, so a database
//! that accepts the connection and never answers its startup would
//! hold `Pg::open`, the readers' connect and every reconnect forever.
//! The proof needs no database: a listener that accepts the TCP
//! connection and never answers.

use std::time::Duration;

use scion_server::test_support::pg::connect_on_deadline_for_tests;

/// A listener that accepts the connection and never answers: the
/// black hole the issue describes. The accept loop runs on its own
/// thread with its own end, so a failed test never parks the suite.
fn black_hole() -> (std::net::SocketAddr, std::thread::JoinHandle<()>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a port to listen on");
    let address = listener.local_addr().unwrap();
    let handle = std::thread::spawn(move || {
        // Each accept gets its connection and never answers it; the
        // loop ends when the listener's socket dies with the test
        // process.
        let deadline = std::time::Instant::now() + Duration::from_secs(120);
        while std::time::Instant::now() < deadline {
            match listener.accept() {
                Ok((_socket, _)) => {
                    // The connection is dropped unread: a `drop` here
                    // would close it, which is an answer of a kind; the
                    // black hole holds it instead, until the socket
                    // dies with the process. So the socket is leaked on
                    // purpose: `std::mem::forget` of a `TcpStream`'
                    // file descriptor is not needed - moving it into a
                    // `Vec` that never shrinks parks it.
                    std::thread::park();
                }
                Err(_) => break,
            }
        }
    });
    (address, handle)
}

/// The open of a database that accepts the connection and never
/// answers its startup fails within the bound, with an error that
/// names the host and the connect phase it was in.
#[test]
fn a_startup_that_never_answers_fails_inside_the_bound() {
    let (address, listener) = black_hole();
    let url = format!(
        "postgres://postgres:@127.0.0.1:{}/scion?sslmode=disable",
        address.port()
    );
    let started = std::time::Instant::now();
    let bound = Duration::from_secs(2);
    let answer = connect_on_deadline_for_tests(&url, None, "", bound);
    let waited = started.elapsed();
    let Err(error) = answer else {
        panic!("the black hole never answers the startup")
    };
    let why = error.to_string();
    assert!(
        why.contains("did not finish its connect"),
        "the error names the phase: {why}"
    );
    assert!(
        why.contains(&format!("127.0.0.1:{}", address.port())),
        "the error names the host: {why}"
    );
    assert!(
        waited >= bound.saturating_sub(Duration::from_millis(500)),
        "the wait honors the bound, not the failure of a dead socket: {waited:?}"
    );
    assert!(
        waited < bound + Duration::from_secs(5),
        "the wait ends at the bound: {waited:?}"
    );
    // The listener's thread parks on its accepted connection by
    // design; the test process reaps it.
    drop(listener);
}
