//! The bench's JSON stage columns measure the push's parts: `put_ms`
//! is the uploads' `PutBlob`, `create_ms` the `CreateChangeset`, `land_ms`
//! the Land. A merge loop that drops an agent's samples would print
//! 0.0 for a stage while `land_ms` stays honest - exactly the split
//! the capacity work reads - so this test pins all three above zero.

#![cfg(unix)]

use scion_proto::runtime::listen::Limits;
use scion_server::test_support::service;
use scion_server::{Backend, ImportOptions, Quotas, import_git};
use serde_json::Value;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use tokio::net::TcpListener;

const TOKEN: &str = "secret";

/// Runs git in `dir` with fixed identities and no user or system config.
/// Fails the test unless git succeeds.
fn git(dir: &Path, args: &[&str]) {
    let out = Command::new("git")
        .args(args)
        .current_dir(dir)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_AUTHOR_NAME", "Ada")
        .env("GIT_AUTHOR_EMAIL", "ada@example.com")
        .env("GIT_AUTHOR_DATE", "1700000000 +0000")
        .env("GIT_COMMITTER_NAME", "Ada")
        .env("GIT_COMMITTER_EMAIL", "ada@example.com")
        .env("GIT_COMMITTER_DATE", "1700000000 +0000")
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// One imported commit, served in-process.
struct Fixture {
    url: String,
    _runtime: tokio::runtime::Runtime,
    _data: tempfile::TempDir,
}

impl Fixture {
    fn start() -> Self {
        let git_dir = tempfile::tempdir().unwrap();
        let dir = git_dir.path();
        std::fs::write(dir.join("seed.txt"), "one\n").unwrap();
        git(dir, &["init", "-q", "-b", "main"]);
        git(dir, &["add", "-A"]);
        git(dir, &["commit", "-q", "-m", "root"]);

        let data = tempfile::tempdir().unwrap();
        let store = Backend::open_test(data.path()).unwrap();
        let opts = ImportOptions {
            rev: "HEAD".into(),
            target_ref: "main".into(),
            verify_sample: 0,
        };
        import_git(&store, "r", dir, &opts).unwrap();

        let runtime = tokio::runtime::Runtime::new().unwrap();
        let listener = runtime.block_on(TcpListener::bind("127.0.0.1:0")).unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        runtime.spawn(service::serve(
            store,
            listener,
            TOKEN,
            service::ServeOptions {
                data: data.path().to_path_buf(),
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
        Self {
            url,
            _runtime: runtime,
            _data: data,
        }
    }

    fn bench_json_with(&self, extra: &[&str]) -> (bool, Value) {
        let out = Command::new(env!("CARGO_BIN_EXE_scn"))
            .args(["--server", &self.url, "--repo", "r", "--json"])
            .args(["bench", "--agents", "2", "--lands", "4"])
            .args(extra)
            .env("SCION_TOKEN", TOKEN)
            .env("SCION_AUTHOR", "Ada <ada@example.com>")
            .output()
            .unwrap();
        let ok = out.status.success();
        (
            ok,
            serde_json::from_str(&String::from_utf8_lossy(&out.stdout)).unwrap_or(Value::Null),
        )
    }

    fn bench_json(&self) -> Value {
        let (ok, json) = self.bench_json_with(&[]);
        assert!(ok, "scn bench: {json}");
        json
    }
}

#[test]
fn the_bench_json_names_a_positive_time_for_every_push_stage() {
    let fixture = Fixture::start();
    let json = fixture.bench_json();
    let landed = json["landed"].as_u64().unwrap();
    assert_eq!(landed, 4, "the bench landed its budget: {json}");
    for stage in ["put_ms", "create_ms", "land_ms", "end_to_end_ms"] {
        let p50 = json[stage]["p50"].as_f64().unwrap_or(-1.0);
        assert!(
            p50 > 0.0,
            "{stage} p50 must measure real work, not 0.0: {json}"
        );
    }
}

#[test]
fn the_bench_json_names_a_positive_rate_and_counts_every_land() {
    let fixture = Fixture::start();
    let json = fixture.bench_json();
    let landed = json["landed"].as_u64().unwrap();
    let seconds = json["seconds"].as_f64().unwrap();
    let rate = json["lands_per_second"].as_f64().unwrap();
    assert!(rate > 0.0, "the rate measures real work: {json}");
    // The rate is the lands over the seconds, not any other combination.
    assert!(
        (rate - landed as f64 / seconds).abs() < 1e-9,
        "lands_per_second must be landed/seconds: {json}"
    );
    // Every land is accounted for: landed plus conflicts plus rejected is
    // the budget.
    let accounted: u64 = ["landed", "conflicts", "rejected"]
        .iter()
        .map(|k| json[k].as_u64().unwrap_or(0))
        .sum();
    assert_eq!(
        accounted, 4,
        "every land lands, conflicts or is rejected: {json}"
    );
}
