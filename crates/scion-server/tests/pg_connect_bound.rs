//! The whole Postgres connect is bounded: the socket connect, its TLS,
//! the startup message and the authentication. tokio-postgres's
//! `connect_timeout` covers only the socket connect, so a database
//! that accepts the connection and never answers its startup would
//! hold `Pg::open`, the readers' connect and every reconnect forever.
//! The proof needs no database: a listener that accepts the TCP
//! connection and never answers. The parked connects the deadline
//! leaves behind are capped, and a listener whose socket goes away
//! gives the slot back.

use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use scion_server::test_support::pg::{connect_on_deadline_for_tests, test_reset_parked_connects};

/// The parked-connect slots are one process-wide seam, and the tests
/// here run in parallel and count them: every test holds the lock for
/// its whole body, so its parked count is its own.
static PARKED_LOCK: Mutex<()> = Mutex::new(());

fn parked_guard() -> MutexGuard<'static, ()> {
    PARKED_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// How long each test's connects wait before they give up.
const BOUND: Duration = Duration::from_millis(300);

/// A listener that accepts the connection and never answers: the black
/// hole the issue describes. Its thread parks on every accepted
/// connection, so the connect's startup never gets an answer; the
/// sockets die with the test process.
fn black_hole() -> std::net::SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a port to listen on");
    let address = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            // The accepted connection is held, never answered: the
            // connect's thread parks on its read until the process
            // ends.
            std::thread::spawn(move || {
                std::thread::park();
                drop(stream);
            });
        }
    });
    address
}

/// A listener that accepts the connection and then closes it: the
/// connect's read fails at once, and the parked slot comes back.
fn closing_hole() -> std::net::SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a port to listen on");
    let address = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(50));
                drop(stream);
            });
        }
    });
    address
}

fn url_of(address: std::net::SocketAddr) -> String {
    format!(
        "postgres://postgres:@127.0.0.1:{}/scion?sslmode=disable",
        address.port()
    )
}

/// The open of a database that accepts the connection and never
/// answers its startup fails within the bound, with an error that
/// names the host and the connect phase it was in.
#[test]
fn a_startup_that_never_answers_fails_inside_the_bound() {
    let _parked = parked_guard();
    test_reset_parked_connects();
    let address = black_hole();
    let url = url_of(address);
    let started = std::time::Instant::now();
    let answer = connect_on_deadline_for_tests(&url, None, "", BOUND);
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
        waited < BOUND + Duration::from_secs(5),
        "the wait ends at the bound: {waited:?}"
    );
}

/// Ten connects against the silent listener: the first two park, the
/// rest fail at once, naming the ceiling - the server does not grow a
/// parked thread and a socket per retry.
#[test]
fn a_third_connect_parked_fails_at_once_naming_the_ceiling() {
    let _parked = parked_guard();
    test_reset_parked_connects();
    let address = black_hole();
    let url = url_of(address);
    // The first connects park: each fails at its bound, having left
    // its thread on the socket. The third's refusal proves the count.
    for _ in 0..2 {
        let started = std::time::Instant::now();
        let Err(error) = connect_on_deadline_for_tests(&url, None, "", BOUND) else {
            panic!("the black hole never answers the startup")
        };
        assert!(
            error.to_string().contains("did not finish its connect"),
            "{error}"
        );
        assert!(
            started.elapsed() >= BOUND,
            "a parking connect waits its bound: {:?}",
            started.elapsed()
        );
    }
    // The next connects fail at once: the cap holds, and the error
    // names the ceiling and the oldest parked connect.
    for _ in 0..8 {
        let started = std::time::Instant::now();
        let Err(error) = connect_on_deadline_for_tests(&url, None, "", BOUND) else {
            panic!("the cap refuses this connect")
        };
        let why = error.to_string();
        assert!(
            why.contains("connects parked on a socket that never answers"),
            "the refusal names the ceiling: {why}"
        );
        assert!(
            why.contains("the oldest for"),
            "the refusal names how long the oldest has waited: {why}"
        );
        assert!(
            started.elapsed() < BOUND,
            "the refused connect does not wait: {:?}",
            started.elapsed()
        );
    }
    // The two parked connects are still parked, and no more: the
    // server's FD count against the dead database stays at the cap.
}

/// A listener whose socket goes away gives the parked slot back: the
/// connect's thread ends on the socket's error, and a later connect
/// takes the slot again.
#[test]
fn a_closed_socket_frees_the_parked_slot() {
    let _parked = parked_guard();
    test_reset_parked_connects();
    let address = closing_hole();
    let url = url_of(address);
    // Each connect's socket closes under it, so its thread ends and
    // gives the slot back: ten of them in a row all run, none refused
    // by the cap.
    for _ in 0..10 {
        let Err(error) = connect_on_deadline_for_tests(&url, None, "", Duration::from_secs(5))
        else {
            panic!("the closing hole never answers the startup")
        };
        assert!(
            !error.to_string().contains("connects parked"),
            "the closed socket frees the slot: {error}"
        );
    }
}
