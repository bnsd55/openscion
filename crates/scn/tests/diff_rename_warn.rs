//! A diff whose rename search hits its bound says so, in its own
//! process: the bound is a process-wide test hook, so the test that
//! holds it runs in its own binary and never races the other diff
//! suites' searches.

use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;

use scion_proto::runtime::listen::Limits;
use scion_server::test_support::meta::Meta as _;
use scion_server::test_support::meta::MetaTx as _;
use scion_server::test_support::service;
use scion_server::{Backend, ImportOptions, import_git};

mod common;
use common::{REPO, TOKEN};

/// A diff whose rename search hits its bound says so: the warning goes
/// to stderr, as git warns when it stops its inexact rename search. A
/// fold in the replies' accumulation silences the warning.
#[test]
fn a_diff_whose_rename_search_hits_its_bound_warns() {
    let git_dir = tempfile::tempdir().unwrap();
    let dir = git_dir.path();
    git(dir, &["init", "-q", "-b", "main"]);
    // Five changed paths in the second commit: the small bound's entry
    // count is three, so the reply says renames may be missing without
    // running a search that a bound this small would starve.
    let mut root: Vec<(&str, Option<&str>)> = (0..4)
        .map(|n| (format!("f{n}.txt").leak() as &str, Some("old")))
        .collect();
    root.push(("gone.txt", Some("bye")));
    root.push(("kept.txt", Some("old")));
    commit(dir, "root", &root);
    let old = git(dir, &["rev-parse", "HEAD"]);
    let mut change: Vec<(&str, Option<&str>)> = (0..4)
        .map(|n| (format!("f{n}.txt").leak() as &str, Some("new")))
        .collect();
    change.push(("gone.txt", None));
    change.push(("added.txt", Some("hi")));
    change.push(("kept.txt", Some("new")));
    commit(dir, "change", &change);
    let new = git(dir, &["rev-parse", "HEAD"]);
    let opts = ImportOptions {
        rev: "HEAD".into(),
        target_ref: "main".into(),
        verify_sample: 0,
    };
    let data = tempfile::tempdir().unwrap();
    let store = Backend::open_test(data.path()).unwrap();
    import_git(&store, REPO, dir, &opts).unwrap();
    let ids = changesets(&store, dir);
    eprintln!("ids: {ids:?}");
    eprintln!("old: {old:?} new: {new:?}");
    let old_id = ids.get(&old).expect("the root commit is imported").clone();
    let new_id = ids
        .get(&new)
        .expect("the change commit is imported")
        .clone();

    // The in-process server, on the runtime this test owns.
    // A multi-thread runtime drives the serve task on its workers; a
    // current-thread one would never run it again after this block.
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let listener = runtime
        .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
        .unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    runtime.spawn(service::serve(
        store,
        listener,
        TOKEN,
        service::ServeOptions {
            data: data.path().to_path_buf(),
            audit: scion_server::test_support::audit::Audit::off(),
            limits: Limits::default(),
            quotas: scion_server::Quotas::off(),
            pending_lands: None,
            land_stale_secs: 1 << 40,
            tls: None,
            metrics: Arc::default(),
            reply_memory_budget: scion_server::REPLY_MEMORY_BUDGET,
        },
        None,
        std::future::pending(),
    ));
    // The runtime lives to the end of the test.
    std::mem::forget(runtime);
    let _small = scion_server::test_support::read::test_set_rename_byte_bound(512);
    let out = Command::new(env!("CARGO_BIN_EXE_scn"))
        .args(["--server", &url, "--repo", REPO, "diff", &old_id, &new_id])
        .env("SCION_TOKEN", TOKEN)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "scn diff: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("inexact rename detection was skipped or stopped by its bound"),
        "the warning names the bound: {stderr}"
    );
}

fn changesets(store: &Backend, dir: &Path) -> HashMap<String, String> {
    let imported = store
        .read(|c| {
            let repo = c.repo_id(REPO)?;
            c.imported_commits(repo)
        })
        .unwrap();
    let oid_of = |commit: &str| -> String {
        let oid: Vec<u8> = (0..commit.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&commit[i..i + 2], 16).unwrap())
            .collect();
        imported
            .iter()
            .find(|(k, _)| k.as_bytes() == oid.as_slice())
            .unwrap()
            .1
            .to_string()
    };
    git(dir, &["rev-list", "--all"])
        .lines()
        .map(|commit| (commit.to_owned(), oid_of(commit)))
        .collect()
}

fn git(dir: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .args(["-c", "gc.auto=0", "-c", "maintenance.auto=false"])
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
    String::from_utf8(out.stdout).unwrap().trim_end().to_owned()
}

fn commit(dir: &Path, message: &str, files: &[(&str, Option<&str>)]) {
    for (name, content) in files {
        let file = dir.join(name);
        match content {
            Some(content) => {
                fs::create_dir_all(file.parent().unwrap()).unwrap();
                fs::write(file, content).unwrap();
            }
            None => fs::remove_file(file).unwrap(),
        }
    }
    git(dir, &["add", "-A"]);
    git(dir, &["commit", "-q", "-m", message]);
}
