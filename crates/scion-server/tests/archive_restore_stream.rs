//! The restore streams its changesets: the frames leave
//! `changesets.bin` one record at a time, and the replay commits one
//! write transaction per byte budget of bodies instead of one
//! transaction for the whole history. The guards: a restore whose
//! transactions are counted, an equality check of the restored store
//! against its source (independent reads, not the restore's own code),
//! a kill mid-replay whose leftovers die at the next restore's start,
//! and - on the nightly - a measured RSS ceiling for a big archive.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

use scion_core::{BlobId, Changeset, ChangesetId, FileState, IdentityTime, RepoPath};
use scion_server::test_support::meta::{Meta as _, MetaTx as _};
use scion_server::{Backend, archive, fsck, test_support::tree};

/// The URL a store on `dir` opens with: the test database, under the
/// directory's own schema, created first.
fn url_of(dir: &std::path::Path) -> String {
    scion_server::Backend::test_postgres_url(dir).unwrap()
}

const CHAIN: usize = 400;
const BUDGET: usize = 16 * 1024;

fn path(p: &str) -> RepoPath {
    RepoPath::new(p).unwrap()
}

fn regular(content: &str) -> FileState {
    FileState::Regular {
        content: BlobId::of(content.as_bytes()),
        executable: false,
    }
}

fn changeset(parents: Vec<ChangesetId>, changes: &[(&str, Option<FileState>)]) -> Changeset {
    changeset_msg(parents, b"change\n".to_vec(), changes)
}

fn changeset_msg(
    parents: Vec<ChangesetId>,
    message: Vec<u8>,
    changes: &[(&str, Option<FileState>)],
) -> Changeset {
    Changeset::new(
        parents,
        IdentityTime::new(
            b"Ada".to_vec(),
            b"ada@example.com".to_vec(),
            1_700_000_000,
            0,
        )
        .expect("identity fits the format"),
        None,
        message,
        changes.iter().map(|(p, s)| (path(p), *s)).collect(),
    )
    .expect("changeset fits the format")
}

/// A source store with `REPOS` repositories of `CHAIN` changesets each:
/// one appended file per changeset, so every body names a real blob and
/// every tree derives. The bodies are the stream's load.
fn stream_source(data: &Path) -> Backend {
    let store = Backend::open_test(data).unwrap();
    for r in 0..2 {
        let name = format!("repo-{r}");
        // The bytes the file holds and the bytes the blob stores are the
        // same string: a change names a blob that exists.
        let contents: Vec<String> = (0..CHAIN)
            .map(|i| format!("repo-{r} file {i}: {}\n", "x".repeat(200)))
            .collect();
        let mut lens = std::collections::HashMap::new();
        let mut blob_ids = Vec::new();
        for content in &contents {
            let id = store.blobs().put(content.as_bytes()).unwrap();
            lens.insert(id, content.len() as u64);
            blob_ids.push(id);
        }
        store
            .write(|c| {
                let repo = c.create_repo(&name)?;
                let mut parent = None::<ChangesetId>;
                for (i, content) in contents.iter().enumerate() {
                    let file = format!("docs/f{i}");
                    let cs = changeset(
                        parent.into_iter().collect(),
                        &[(&file, Some(regular(content)))],
                    );
                    parent =
                        Some(tree::store_changeset(c, repo, &cs, &mut tree::lens_from(&lens))?.0);
                    c.add_repo_blob(repo, blob_ids[i])?;
                }
                assert!(c.cas_ref(repo, "main", None, parent.expect("a chain"))?);
                Ok(())
            })
            .unwrap();
    }
    store
}

/// The full read side of one store, as the equality check sees it:
/// refs with their versions, the whole first-parent chain per ref,
/// the generations, and the root tree of each ref's tip.
fn read_side(store: &Backend) -> BTreeMap<String, Vec<String>> {
    let mut out = BTreeMap::new();
    for (repo_id, name) in store.read(|c| c.repos()).unwrap() {
        let state = store.read(|c| c.get_ref(repo_id, "main")).unwrap().unwrap();
        out.insert(
            format!("{name} ref {} v{}", name, state.version),
            vec![state.target.to_string()],
        );
        // The whole first-parent walk from the tip.
        let mut chain = Vec::new();
        let mut from = state.target;
        loop {
            let page = store
                .read(|c| c.first_parent_page(repo_id, from, 1000))
                .unwrap();
            for (id, generation, _body) in &page {
                chain.push(format!("{id} g{generation}"));
            }
            match page.last() {
                Some((id, _, _)) => from = *id,
                None => break,
            }
            if page.len() < 1000 {
                break;
            }
        }
        out.insert(format!("{name} chain"), chain);
        let root = tree::root_tree_unbounded(store, repo_id, state.target).unwrap();
        out.insert(format!("{name} root"), vec![root.to_string()]);
    }
    out
}

/// Reads both stores the same way and asserts they answer the same.
fn assert_restore_equals_source(source: &Backend, restored: &Backend) {
    assert_eq!(read_side(source), read_side(restored), "the reads match");
    assert_eq!(read_side(source), read_side(restored), "the reads match");
    // A blob's bytes read back the same.
    for (repo_id, name) in source.read(|c| c.repos()).unwrap() {
        let tip = source
            .read(|c| c.get_ref(repo_id, "main"))
            .unwrap()
            .unwrap()
            .target;
        let body = source.read(|c| c.get_changeset(repo_id, tip)).unwrap();
        let restored_repo = restored.read(|c| c.repo_id(&name)).unwrap();
        let restored_body = restored
            .read(|c| c.get_changeset(restored_repo, tip))
            .unwrap();
        assert_eq!(body, restored_body, "the tip's body reads the same");
    }
}

#[test]
fn a_restore_commits_bounded_transactions_and_answers_like_its_source() {
    let data = scion_server::test_support::private_tempdir();
    let store = stream_source(data.path());

    let out = scion_server::test_support::private_tempdir()
        .path()
        .join("archive");
    let manifest = archive::export(&store, data.path(), None, true, &out)
        .unwrap_or_else(|e| panic!("export failed: {e:#}"));
    assert_eq!(manifest["kind"], "store");
    let damages = archive::verify(&out).unwrap();
    assert!(damages.is_empty(), "{damages:?}");

    // The small budget: the two repositories' bodies stream through
    // many transactions. The count comes from the counter, read before
    // and after, so the assertion is about work done, not work planned.
    let before = scion_server::test_support::archive::restore_tx_count();
    let restored_dir = scion_server::test_support::private_tempdir();
    archive::restore_with_budget(
        &out,
        restored_dir.path(),
        &url_of(restored_dir.path()),
        None,
        BUDGET,
        Backend::open,
    )
    .unwrap_or_else(|e| panic!("restore failed: {e:#}"));
    let txs = scion_server::test_support::archive::restore_tx_count() - before;
    println!("the restore committed {txs} transactions");
    assert!(
        txs > 2,
        "the restore committed {txs} transactions; the stream did not split"
    );

    // Every read equals the source's, and both stores are clean.
    let restored = Backend::open(restored_dir.path(), &url_of(restored_dir.path()), None).unwrap();
    assert_restore_equals_source(&store, &restored);
    for store in [&store, &restored] {
        let problems = fsck::run(store, fsck::BATCH, &mut |_| {}).unwrap();
        assert!(problems.problems.is_empty(), "{problems:?}");
    }
}

/// The nightly ceiling: a big archive, restored by the `scion-server
/// restore` subcommand in its own process, whose peak RSS the child
/// names itself (its own kernel high-water mark, printed at the end).
/// The bound is self-calibrating: the test first measures an empty
/// archive's restore in a twin process - that peak is the binary's
/// own footprint, whatever platform it runs on - and the big restore
/// must stay under the twin's baseline plus the replay's OWN budget.
/// The replay commits per byte budget and holds the batch's decoded
/// changesets, so its memory is a function of the budget, not of the
/// history - the test pins the budget low (4 MiB, the
/// `SCION_TEST_RESTORE_TX_BYTES` lever) so a whole-history read cannot
/// hide inside an allowance sized like a production budget. The
/// archive's `changesets.bin` must be at least twice that budget, so
/// the old whole-file read (unbudgeted, the file whole) would blow
/// the bound and the guard is not vacuous. `SCION_TEST_RESTORE_BIG`
/// arms it, and it runs only in a release build on CI (the nightly,
/// main and a dispatch): the fixture holds too much store for a
/// shared dev machine. The size rides the changesets' messages: a few
/// changesets with big bodies fill the archive without the store a
/// long chain would grow.
#[test]
fn a_big_restore_stays_under_its_rss_ceiling() {
    const CHAIN_BIG: usize = 64;
    const MESSAGE_BYTES: usize = 200 * 1024;
    // The replay's budget the test pins: 4 MiB of bodies per commit,
    // the batch holds no more.
    const TX_KB: u64 = 4 * 1024;
    // One record over the batch, the decode, the store's page cache,
    // the margin. The bound is the twin's baseline + TX_KB + this;
    // the measure lives on Linux, so hosts that skip it hold the name
    // only.
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    const SLACK_KB: u64 = 8 * 1024;

    if std::env::var_os("SCION_TEST_RESTORE_BIG").is_none() {
        println!("skipping the big restore: SCION_TEST_RESTORE_BIG arms it");
        return;
    }

    let data = scion_server::test_support::private_tempdir();
    let store = Backend::open_test(data.path()).unwrap();
    let content = "small file\n".to_string();
    let blob = store.blobs().put(content.as_bytes()).unwrap();
    let mut lens = std::collections::HashMap::new();
    lens.insert(blob, content.len() as u64);
    store
        .write(|c| {
            let repo = c.create_repo("big")?;
            let mut parent = None::<ChangesetId>;
            for i in 0..CHAIN_BIG {
                let cs = changeset_msg(
                    parent.into_iter().collect(),
                    vec![b'x'; MESSAGE_BYTES],
                    &[(&format!("docs/f{i}"), Some(regular(&content)))],
                );
                let id = tree::store_changeset(c, repo, &cs, &mut tree::lens_from(&lens))?.0;
                c.add_repo_blob(repo, blob)?;
                parent = Some(id);
            }
            assert!(c.cas_ref(repo, "main", None, parent.expect("a chain"))?);
            Ok(())
        })
        .unwrap();

    let out = scion_server::test_support::private_tempdir()
        .path()
        .join("archive");
    archive::export(&store, data.path(), None, true, &out)
        .unwrap_or_else(|e| panic!("export failed: {e:#}"));
    let bin_len = std::fs::metadata(out.join("repos").join("big").join("changesets.bin"))
        .unwrap()
        .len()
        / 1024;
    println!("changesets.bin is {bin_len} KiB; the budget is {TX_KB} KiB");
    assert!(
        bin_len >= 2 * TX_KB,
        "changesets.bin ({bin_len} KiB) must be at least twice the replay's budget \
         ({TX_KB} KiB), or the old whole-file read would fit under it"
    );

    // A restore runs as its own process, so the peak RSS measured is
    // the restore's, not the test fixture's. The twin first: an empty
    // archive, restored the same way - its peak is the binary's own
    // footprint on this machine, and the bound rides on it. The
    // ceiling runs where the CI job runs it: Linux.
    #[cfg(target_os = "linux")]
    measure_ceiling(&store, &out, TX_KB, SLACK_KB);
    #[cfg(not(target_os = "linux"))]
    println!("the ceiling measures wait4 peaks on Linux only; skipping here");
}

/// The ceiling's own measure and verdict: the twin's baseline, the big
/// restore's peak under baseline + slack, the restored store answering
/// like the source, and the fsck that says so.
#[cfg(target_os = "linux")]
fn measure_ceiling(store: &Backend, out: &std::path::Path, tx_kb: u64, slack_kb: u64) {
    let baseline_kb = restore_sampled(
        &empty_archive(),
        &scion_server::test_support::private_tempdir()
            .path()
            .join("restored-empty"),
        tx_kb * 1024,
    );
    let target = scion_server::test_support::private_tempdir()
        .path()
        .join("restored");
    let peak_kb = restore_sampled(out, &target, tx_kb * 1024);
    println!(
        "the streamed restore's peak RSS was {peak_kb} kB; \
         the bound is the twin's baseline {baseline_kb} kB + the replay's budget \
         {tx_kb} kB + the slack {slack_kb} kB"
    );
    assert!(
        peak_kb <= baseline_kb + tx_kb + slack_kb,
        "the restore's peak RSS ({peak_kb} kB) passed the ceiling \
         (baseline {baseline_kb} kB + budget {tx_kb} kB + slack {slack_kb} kB); \
         a whole-history read is back"
    );

    // The restored store answers like the source.
    let restored = Backend::open(&target, &url_of(&target), None).unwrap();
    assert_restore_equals_source(store, &restored);
    let problems = fsck::run(&restored, fsck::BATCH, &mut |_| {}).unwrap();
    assert!(problems.problems.is_empty(), "{problems:?}");
}

/// A restore killed mid-replay leaves its committed batches behind, in
/// the database the URL names. The next restore cleans them at its
/// START - a kill runs no failure path - and lands whole.
#[test]
fn a_killed_restore_leaves_the_next_one_clean() {
    let data = scion_server::test_support::private_tempdir();
    let store = stream_source(data.path());
    let out = scion_server::test_support::private_tempdir()
        .path()
        .join("archive");
    archive::export(&store, data.path(), None, true, &out)
        .unwrap_or_else(|e| panic!("export failed: {e:#}"));

    let target = scion_server::test_support::private_tempdir()
        .path()
        .join("restored");
    std::fs::create_dir_all(&target).unwrap();
    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .arg("--data")
        .arg(&target)
        // The restore reads its URL from the environment: the target's
        // own schema, so the replay lands where the open below reads.
        .env("SCION_POSTGRES_URL", url_of(&target))
        .env("SCION_TEST_RESTORE_TX_BYTES", "4096")
        .env("SCION_TEST_RESTORE_ECHO_BATCH", "1")
        .arg("restore")
        .arg("--archive")
        .arg(&out)
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    // The kill waits for real work: the second committed batch on the
    // child's own stderr, then SIGKILL. If the restore finishes first,
    // the test still holds - the kill then hits a finished run, and the
    // second restore's precondition (a data directory that holds a
    // store) refuses it, so the test says so and moves on.
    let stderr = child.stderr.take().unwrap();
    let reader = std::thread::spawn(move || {
        use std::io::BufRead as _;
        let reader = std::io::BufReader::new(stderr);
        let mut text = String::new();
        let mut seen = false;
        for line in reader.lines() {
            let line = line.unwrap_or_default();
            text.push_str(&line);
            text.push('\n');
            if line.contains("batch 2 committed") {
                seen = true;
                break;
            }
        }
        // The kill must hit real work: a restore that died at startup
        // (a missing URL, a refused open) closes its stderr before any
        // batch, and a test that killed it anyway would prove nothing.
        assert!(seen, "the restore never reached its second batch: {text}");
    });
    let start = std::time::Instant::now();
    loop {
        if reader.is_finished() {
            break;
        }
        assert!(
            start.elapsed() < Duration::from_secs(120),
            "the restore never reached its second batch"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    child.kill().unwrap();
    child.wait().unwrap();

    // The next restore starts over and lands whole.
    let report = archive::restore(&out, &target, &url_of(&target), None)
        .unwrap_or_else(|e| panic!("the restore after the kill failed: {e:#}"));
    assert_eq!(report.repos.len(), 2);
    let restored = Backend::open(&target, &url_of(&target), None).unwrap();
    assert_restore_equals_source(&store, &restored);
    let problems = fsck::run(&restored, fsck::BATCH, &mut |_| {}).unwrap();
    assert!(problems.problems.is_empty(), "{problems:?}");
}

/// An archive of a store with no repos: the twin the ceiling test
/// restores to measure the binary's own footprint.
#[cfg(target_os = "linux")]
fn empty_archive() -> std::path::PathBuf {
    let data = scion_server::test_support::private_tempdir();
    let store = Backend::open_test(data.path()).unwrap();
    let parent = scion_server::test_support::private_tempdir();
    let out = parent.path().join("archive-empty");
    archive::export(&store, data.path(), None, true, &out).unwrap();
    drop(store);
    let manifest = std::fs::read_to_string(out.join("manifest.json")).unwrap();
    assert!(
        manifest.contains(r#""repos": []"#),
        "the twin's archive is empty: {manifest}"
    );
    // Leak the parents: the archive must outlive the test's restores.
    std::mem::forget(data);
    std::mem::forget(parent);
    out
}

/// A restore in its own process, whose exact peak RSS the child
/// prints at its very end: the restore subcommand's measurement seam
/// (`SCION_TEST_RESTORE_PEAK`, test-hooks only) reads the kernel's own
/// high-water mark (`VmHWM`, KiB) when the run is over - no sampling,
/// no race, a short process's peak as exact as a long one's.
#[cfg(target_os = "linux")]
fn restore_sampled(archive: &std::path::Path, target: &std::path::Path, tx_bytes: u64) -> u64 {
    std::fs::create_dir_all(target).unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .arg("--data")
        .arg(target)
        // The restore reads its URL from the environment: the target's
        // own schema, so the reset and the replay stay in one schema.
        .env("SCION_POSTGRES_URL", url_of(target))
        .arg("restore")
        .arg("--archive")
        .arg(archive)
        .env("SCION_TEST_RESTORE_TX_BYTES", tx_bytes.to_string())
        .env("SCION_TEST_RESTORE_PEAK", "1")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "the restore failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let peaks: Vec<&str> = stdout
        .lines()
        .filter_map(|line| line.strip_prefix("restore: peak RSS "))
        .collect();
    assert_eq!(peaks.len(), 1, "the child named its peak once: {stdout}");
    // A peak line that does not parse is a bug in the seam, not a
    // small process: fail loudly instead of measuring zero.
    let peak_text = peaks[0].trim();
    peak_text
        .parse()
        .unwrap_or_else(|e| panic!("the child's peak line does not parse: {peak_text:?}: {e}"))
}

/// The truncation lands LATE in the stream: the first batch is already
/// committed when the reader walks into the cut. The design (archive.rs,
/// `restore_impl`): every batch lands in a staging directory beside the
/// target, and the target receives the store by ONE rename only after
/// the whole replay. A late truncation must therefore fail the restore
/// and leave the target with NO store a server would open - the answer
/// the operator sees is a refused restore, not a half store.
#[test]
fn a_stream_cut_after_the_first_batch_fails_and_leaves_no_store() {
    const CHAIN_BIG: usize = 64;
    const MESSAGE_BYTES: usize = 200 * 1024;

    let data = scion_server::test_support::private_tempdir();
    let store = Backend::open_test(data.path()).unwrap();
    let content = "small file\n".to_string();
    let blob = store.blobs().put(content.as_bytes()).unwrap();
    let mut lens = std::collections::HashMap::new();
    lens.insert(blob, content.len() as u64);
    store
        .write(|c| {
            let repo = c.create_repo("big")?;
            let mut parent = None::<ChangesetId>;
            for i in 0..CHAIN_BIG {
                let cs = changeset_msg(
                    parent.into_iter().collect(),
                    vec![b'x'; MESSAGE_BYTES],
                    &[(&format!("docs/f{i}"), Some(regular(&content)))],
                );
                let id = tree::store_changeset(c, repo, &cs, &mut tree::lens_from(&lens))?.0;
                c.add_repo_blob(repo, blob)?;
                parent = Some(id);
            }
            assert!(c.cas_ref(repo, "main", None, parent.expect("a chain"))?);
            Ok(())
        })
        .unwrap();

    let out = tempfile::tempdir().unwrap().path().join("archive");
    archive::export(&store, data.path(), None, true, &out)
        .unwrap_or_else(|e| panic!("export failed: {e:#}"));
    let victim = out.join("repos").join("big").join("changesets.bin");
    let full_len = std::fs::metadata(&victim).unwrap().len();
    assert!(
        full_len > 8 * BUDGET as u64,
        "the stream ({full_len} B) must hold several batches (BUDGET {BUDGET} B), \
         or the cut cannot land past the first batch"
    );

    // Cut at two budgets in: the first batch (and more) committed, the
    // reader walks into a record the file no longer holds.
    let f = std::fs::OpenOptions::new()
        .write(true)
        .open(&victim)
        .unwrap();
    f.set_len(2 * BUDGET as u64).unwrap();
    drop(f);

    let target = tempfile::tempdir().unwrap().path().join("target");
    let error =
        archive::restore_with_budget(&out, &target, &url_of(&target), None, BUDGET, Backend::open)
            .expect_err("a cut stream must not restore");
    assert!(
        error.to_string().contains("ends inside a record"),
        "the refusal must name the torn stream: {error}"
    );

    // The target holds no store a server would open: the marker, the
    // database and the objects folder are all absent, and opening the
    // directory as a store refuses.
    for existing in ["scion.db", "backend", "objects"] {
        assert!(
            !target.join(existing).exists(),
            "the target must not hold {existing} after a refused restore"
        );
    }
    // (Backend::open on an empty directory CREATES a fresh store by
    // design - it is not an oracle here.) The staging scratch went with
    // the failure, and the target is empty for the operator's retry: a
    // half store would name itself in the listing.
    let staging = target.with_file_name(format!(
        "{}.restoring",
        target.file_name().unwrap().to_string_lossy()
    ));
    assert!(!staging.exists(), "the staging scratch must be gone");
    // A target that was never made at all is the same answer: nothing
    // of the restore became visible.
    let left: Vec<_> = match std::fs::read_dir(&target) {
        Ok(entries) => entries.collect(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(error) => panic!("the target is unreadable: {error}"),
    };
    assert!(left.is_empty(), "the target must be empty: {left:?}");
}
