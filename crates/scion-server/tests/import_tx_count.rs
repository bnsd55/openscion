//! The import's write transactions: the batch takes a whole chain in one
//! write transaction, and the per-commit knob (a test hook) takes one
//! commit per transaction. Both shapes are observable in the statement
//! log, and the log is a process-wide seam, so this guard owns its
//! binary: no other test in it runs statements.

use std::io::Write;
use std::process::{Command, Stdio};

use scion_server::{Backend, ImportOptions, import_git};

/// A throwaway git repository. `fast_commits` appends a chain of commits
/// on `main` with `git fast-import`, which is fast for long histories.
struct Git(tempfile::TempDir);

impl Git {
    fn new() -> Self {
        let dir = scion_server::test_support::private_tempdir();
        let out = Command::new("git")
            .args(["init", "-q", "-b", "main"])
            .current_dir(dir.path())
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .output()
            .unwrap();
        assert!(out.status.success());
        Self(dir)
    }

    /// Appends commits `from..to` on `main`, one file each. Returns the tip.
    fn fast_commits(&self, from: usize, to: usize) -> String {
        let mut stream = Vec::new();
        for i in from..to {
            let (message, content) = (format!("c{i}\n"), format!("{i}\n"));
            let when = 1_700_000_000 + i;
            writeln!(stream, "commit refs/heads/main").unwrap();
            writeln!(stream, "committer Bob <bob@example.com> {when} +0000").unwrap();
            write!(stream, "data {}\n{message}", message.len()).unwrap();
            if i == from && from > 0 {
                writeln!(stream, "from refs/heads/main^0").unwrap();
            }
            writeln!(stream, "M 100644 inline g/f{i}").unwrap();
            writeln!(stream, "data {}\n{content}", content.len()).unwrap();
        }
        let mut child = Command::new("git")
            .args(["fast-import", "--quiet"])
            .current_dir(self.0.path())
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child.stdin.take().unwrap().write_all(&stream).unwrap();
        let out = child.wait_with_output().unwrap();
        assert!(
            out.status.success(),
            "git fast-import: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8(
            Command::new("git")
                .args(["rev-parse", "main"])
                .current_dir(self.0.path())
                .env("GIT_CONFIG_GLOBAL", "/dev/null")
                .output()
                .unwrap()
                .stdout,
        )
        .unwrap()
        .trim_end()
        .to_owned()
    }

    fn path(&self) -> &std::path::Path {
        self.0.path()
    }
}

fn opts(rev: &str) -> ImportOptions {
    ImportOptions {
        rev: rev.to_owned(),
        target_ref: "main".to_owned(),
        verify_sample: 0,
    }
}

/// The provenance writes of one import: one statement per write
/// transaction (the batch's one provenance write at its end).
fn provenance_writes() -> usize {
    Backend::test_statement_log()
        .iter()
        .filter(|sql| sql.contains("INSERT INTO git_import"))
        .count()
}

#[test]
fn the_batch_takes_one_transaction_for_the_chain_and_the_knob_one_per_commit() {
    let git = Git::new();
    let tip = git.fast_commits(0, 40);
    let dir = scion_server::test_support::private_tempdir();
    let store = Backend::open_test(dir.path()).unwrap();

    // Batched: the whole chain in one write transaction, whatever the
    // machine's speed - the budget only ends a transaction after a second
    // of work, and 40 commits cost far less. A break that fires on the
    // first commit takes one commit per transaction and fails here.
    Backend::test_reset_statement_count();
    let report = import_git(&store, "main", git.path(), &opts(&tip)).unwrap();
    assert_eq!(report.imported, 40);
    let txes = provenance_writes();
    assert!(txes >= 1, "the import wrote no provenance rows");
    assert!(
        txes <= 10,
        "the import of 40 commits wrote its provenance in {txes} \
         transactions, the shape of one commit per transaction"
    );

    // Per commit: the knob takes one commit per write transaction, so ten
    // new commits cost ten provenance writes. A setter that stores
    // nothing leaves the batch shape and fails here.
    let tip = git.fast_commits(40, 50);
    scion_server::set_import_per_commit_txs(true);
    Backend::test_reset_statement_count();
    let report = import_git(&store, "main", git.path(), &opts(&tip)).unwrap();
    scion_server::set_import_per_commit_txs(false);
    assert_eq!(report.imported, 10);
    let txes = provenance_writes();
    assert_eq!(
        txes, 10,
        "one provenance write per commit under the per-commit knob"
    );
}
