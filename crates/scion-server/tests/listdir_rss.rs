//! The server's RSS stays bounded while many clients hold unread
//! `ListDir` replies at once: the whole-object share is taken from the
//! stored tree body's length BEFORE the body is built, so a spent read
//! budget queues or refuses a build it cannot pay for. Red before the
//! rule: the old order built the body first and took its share after,
//! so every concurrent build sat in RAM uncounted (the `whole_share`
//! shortcut this suite retires).
//!
//! The budget that pays for a `ListDir` reply is the READ budget
//! (`READ_BUDGET_BYTES`, 256 MiB), not the unary reply budget: the
//! reply streams, so the reply-memory layer skips it.
//!
//! Needs `/proc` (Linux): the measurement is the child server's
//! `VmRSS`.

#![cfg(target_os = "linux")]

use scion_core::{Changeset, ChangesetId, FileState, IdentityTime, RepoPath};
use scion_proto::runtime::auth::Bearer;
use scion_proto::v1::ListDirRequest;
use scion_proto::v1::scion_client::ScionClient;
use scion_server::test_support::meta::MetaTx;
use scion_server::test_support::store::Store;

mod common;

const REPO: &str = "main";
const TOKEN: &str = "secret";

/// One directory wide enough that one `ListDir` reply's tree body is
/// about 2 MB (13,000 entries of 120 characters).
const NAMES: usize = 13_000;
/// Held unread streams: 360 bodies of about 2 MB is about 749 MB of
/// live bytes under the old order, more than the whole ceiling
/// (baseline + two budgets + slack), so the old order passes it by its
/// live bytes alone - the red cannot ride on how the allocator treats
/// freed copies. The right order caps live bodies at the budget
/// (about 130), and its measured run (CI 40103512403) lands at 545 MB
/// over baseline, under that ceiling.
const STREAMS: usize = 360;
/// The streams ride 18 connections: 20 replies of 2 MB is about 42 MB
/// per connection, under the 64 MiB stalled-bytes cap that would
/// otherwise close a connection and free what it holds - the test must
/// measure the read budget, not the stall cap.
const CONNECTIONS: usize = 18;
/// The ceiling is baseline + TWO read budgets + slack: one budget is
/// the held bodies' own bytes, the second is the transient copy each
/// build makes while it encodes (the allocator keeps freed 2 MB blocks
/// on both glibc and macOS, so a held body costs its bytes twice
/// before the run settles). CI's first printed line measured 2.03x the
/// held bytes.
const CEILING_BUDGETS: u64 = 2;
/// The slack: the worker's own buffers, tonic's frames, malloc's book.
const SLACK_KB: u64 = 64 * 1024;
/// A call whose share queues waits for a held body that its client
/// never reads, so it parks until this timeout. A parked call is the
/// design: the budget queues the build, it does not skip it.
const OPEN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(20);

fn author() -> IdentityTime {
    IdentityTime::new(b"t".to_vec(), b"t@x".to_vec(), 1_700_000_000, 0)
        .expect("identity fits the format")
}

fn wide_name(i: usize) -> RepoPath {
    let mut name = format!("entry-{i:06}.").into_bytes();
    name.resize(120, b'a');
    RepoPath::new(name.as_slice()).unwrap()
}

/// Seeds an on-disk store whose root directory holds `NAMES` entries.
fn seed(dir: &std::path::Path) -> ChangesetId {
    let store = Store::open(dir).unwrap();
    let blob = store.blobs().put(b"payload\n").unwrap();
    let changeset = Changeset::new(
        vec![],
        author(),
        None,
        b"wide\n".to_vec(),
        (0..NAMES)
            .map(|i| {
                (
                    wide_name(i),
                    Some(FileState::Regular {
                        content: blob,
                        executable: false,
                    }),
                )
            })
            .collect(),
    )
    .expect("changeset fits the format");
    store
        .write(|c| {
            let repo = c.create_repo(REPO)?;
            c.insert_changeset(repo, &changeset)
        })
        .unwrap()
}

/// The child server's resident set in kB, from `/proc` where it
/// exists and from `ps` where it does not; 0 when neither answers.
fn rss_kb(pid: u32) -> u64 {
    let status = format!("/proc/{pid}/status");
    if let Ok(text) = std::fs::read_to_string(status) {
        for line in text.lines() {
            if let Some(kb) = line
                .strip_prefix("VmRSS:")
                .and_then(|kb| kb.trim().trim_end_matches("kB").trim().parse::<u64>().ok())
            {
                return kb;
            }
        }
    }
    std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .ok()
        .and_then(|out| {
            String::from_utf8_lossy(&out.stdout)
                .trim()
                .parse::<u64>()
                .ok()
        })
        .unwrap_or(0)
}

/// A running real `scion-server` on the seeded store.
struct Server {
    child: std::process::Child,
    addr: std::net::SocketAddr,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn serve(dir: &std::path::Path) -> Server {
    let log = std::fs::File::create(dir.join("server.log")).unwrap();
    let child = std::process::Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .arg("--data")
        .arg(dir)
        .args(["serve", "--token", TOKEN, "--listen", "127.0.0.1:0"])
        .env("NO_COLOR", "1")
        .env_remove("RUST_LOG")
        // The mutation lever rides through to the child, for the red.
        .env(
            "SCION_TEST_LISTDIR_MUTATION",
            std::env::var("SCION_TEST_LISTDIR_MUTATION").unwrap_or_default(),
        )
        .stdout(std::process::Stdio::from(log.try_clone().unwrap()))
        .stderr(std::process::Stdio::from(log))
        .spawn()
        .unwrap();
    let log_path = dir.join("server.log");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let addr = loop {
        if let Some(addr) = bound_addr(&log_path) {
            break addr;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the server never announced its address:\n{}",
            std::fs::read_to_string(&log_path).unwrap_or_default()
        );
        std::thread::sleep(std::time::Duration::from_millis(50));
    };
    Server { child, addr }
}

/// The `listen=` address of the newest `serving` line in the log.
fn bound_addr(log: &std::path::Path) -> Option<std::net::SocketAddr> {
    let text = std::fs::read_to_string(log).ok()?;
    text.lines()
        .filter(|line| line.contains("serving"))
        .filter_map(|line| line.rsplit("listen=").next())
        .next_back()
        .and_then(|addr| addr.split_whitespace().next())
        .and_then(|addr| addr.parse().ok())
}

#[tokio::test(flavor = "multi_thread")]
async fn held_list_dir_replies_stay_under_the_budget_in_rss() {
    let dir = tempfile::tempdir().unwrap();
    let changeset = seed(dir.path());
    let changeset_bytes = changeset.as_bytes().to_vec();
    let server = serve(dir.path());

    let baseline_kb = rss_kb(server.child.id());
    assert!(
        baseline_kb > 0,
        "the sampler saw no RSS; this test needs /proc"
    );

    // The streams ride several connections, under each one's
    // stalled-bytes cap, so the read budget is the only bound at work.
    let mut clients = Vec::new();
    for _ in 0..CONNECTIONS {
        let client = ScionClient::with_interceptor(
            common::channel(&format!("http://{}", server.addr))
                .await
                .unwrap(),
            Bearer::new(TOKEN).unwrap(),
        );
        clients.push(client);
    }

    // Open every stream at once, and HOLD whatever opens: the first
    // builds take their shares and hold their bodies; the rest park on
    // the spent budget. A parked call is the design, not a failure.
    // The holds stay live across the measurement: the measurement must
    // see the bodies while the server holds them, not after the
    // clients walked away and the bytes went free.
    let mut tasks = tokio::task::JoinSet::new();
    for i in 0..STREAMS {
        let changeset_bytes = changeset_bytes.clone();
        let mut client = clients[i % CONNECTIONS].clone();
        tasks.spawn(async move {
            let request = ListDirRequest {
                repo: REPO.into(),
                changeset_id: changeset_bytes,
                path: Vec::new(),
            };
            match tokio::time::timeout(OPEN_TIMEOUT, client.list_dir(request)).await {
                Ok(Ok(response)) => {
                    let stream = response.into_inner();
                    // Hold the stream: its reply is never read, and
                    // the hold never ends on its own.
                    std::future::pending::<()>().await;
                    drop(stream);
                    true
                }
                _ => false,
            }
        });
    }
    // The opens settle: the first builds land inside a second, the
    // parked calls sit on the budget. Wait past the open timeout so
    // every stream has opened or parked, then measure while the holds
    // are live.
    tokio::time::sleep(OPEN_TIMEOUT + std::time::Duration::from_secs(5)).await;
    let after_kb = rss_kb(server.child.id());
    assert!(after_kb > 0, "the server stays up and reports its RSS");
    // The holds stay live through the asserts below; the JoinSet's
    // drop (the test's end) aborts them. Joining after an abort panics
    // on the cancelled tasks, so the set is only ever dropped.
    let budget_kb = (scion_server::READ_BUDGET_BYTES / 1024) as u64;
    let ceiling = baseline_kb + CEILING_BUDGETS * budget_kb + SLACK_KB;
    let floor = baseline_kb + budget_kb / 2;
    println!(
        "baseline {baseline_kb} kB, after {after_kb} kB, floor {floor} kB, ceiling {ceiling} kB"
    );
    assert!(
        after_kb >= floor,
        "the server's RSS ({after_kb} kB) stayed under the floor ({floor} kB); \
         the holds never spent the budget, so this proves nothing"
    );
    assert!(
        after_kb <= ceiling,
        "the server's RSS ({after_kb} kB) passed the ceiling ({ceiling} kB); \
         replies are built outside the budget"
    );
}
