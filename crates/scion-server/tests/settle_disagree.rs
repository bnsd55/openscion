//! A settle refuses when a stored derived value differs from the one
//! the build computes: the derived row of a changeset this transaction
//! did not write is not this transaction's to answer, so a short
//! insert on the settle's write means corrupt state (#1004). The
//! check must see the stored row before the transaction's memory
//! follows its writes - the settle's one statement checks inside, so
//! the check cannot read back the value it just wrote.

use scion_core::{Changeset, FileState, RepoPath};
use scion_server::Backend;
use scion_server::test_support::meta::{Meta as _, MetaTx as _};
use scion_server::test_support::tree;
use std::collections::BTreeMap;

const REPO: &str = "disagree";

fn identity() -> scion_core::IdentityTime {
    scion_core::IdentityTime::new(
        b"Ada".to_vec(),
        b"ada@example.com".to_vec(),
        1_700_000_000,
        0,
    )
    .expect("identity fits the format")
}

/// A store with repo `disagree` and `cs` stored: a root changeset that
/// puts one file, the settle's own shape.
fn a_root_changeset() -> (tempfile::TempDir, Backend, scion_core::ChangesetId) {
    let dir = scion_server::test_support::private_tempdir();
    let store = Backend::open_test(dir.path()).unwrap();
    store.blobs().put(b"f\n".as_slice()).unwrap();
    let cs = Changeset::new(
        Vec::new(),
        identity(),
        None,
        b"root\n".to_vec(),
        BTreeMap::from([(
            RepoPath::new("f").unwrap(),
            Some(FileState::Regular {
                content: scion_core::BlobId::of(b"f\n"),
                executable: false,
            }),
        )]),
    )
    .expect("changeset fits the format");
    let cs = store
        .write(|c| {
            let repo = c.create_repo(REPO)?;
            let (cs, _) = tree::store_changeset(c, repo, &cs, &mut |_| Ok(0))?;
            Ok(cs)
        })
        .unwrap();
    (dir, store, cs)
}

/// Rewrites the stored derived `kind` value of `cs` through the test
/// schema's own client, the way a torn write would leave it.
fn corrupt_derived(dir: &std::path::Path, kind: &str, cs: &scion_core::ChangesetId) {
    let schema = Backend::test_schema(dir);
    let url = std::env::var("SCION_TEST_POSTGRES_URL").unwrap();
    let mut client = postgres::Client::connect(&url, postgres::NoTls).unwrap();
    client
        .batch_execute(&format!("SET search_path TO {schema}"))
        .unwrap();
    let key = cs.as_bytes().to_vec();
    let changed = client
        .execute(
            "UPDATE derived SET value = value || 'x' WHERE kind = $1 AND cs = $2",
            &[&kind, &key],
        )
        .unwrap();
    assert_eq!(changed, 1, "the corruption changed a row");
}

/// The same changeset stored again: every row is already there, so the
/// settle's write conflicts and the stored values must agree with the
/// computed ones - a stored root tree that differs refuses as corrupt,
/// and the transaction rolls back whole.
#[test]
fn a_settled_changeset_whose_stored_root_tree_differs_is_refused() {
    let (dir, store, cs) = a_root_changeset();
    corrupt_derived(dir.path(), "root_tree", &cs);
    let repo = store.read(|c| c.repo_id(REPO)).unwrap();
    let again = Changeset::new(
        Vec::new(),
        identity(),
        None,
        b"root\n".to_vec(),
        BTreeMap::from([(
            RepoPath::new("f").unwrap(),
            Some(FileState::Regular {
                content: scion_core::BlobId::of(b"f\n"),
                executable: false,
            }),
        )]),
    )
    .expect("changeset fits the format");
    // The cs's tree rows go away first, so the refused settle's tree
    // arm would insert fresh rows: what survives the refusal is what
    // proves the rollback, since the same rows hit DO NOTHING when
    // they are already there.
    let schema = Backend::test_schema(dir.path());
    let url = std::env::var("SCION_TEST_POSTGRES_URL").unwrap();
    let mut client = postgres::Client::connect(&url, postgres::NoTls).unwrap();
    client
        .batch_execute(&format!("SET search_path TO {schema}"))
        .unwrap();
    client.execute("DELETE FROM trees", &[]).unwrap();
    let error = store
        .write(|c| tree::store_changeset(c, repo, &again, &mut |_| Ok(0)))
        .unwrap_err();
    assert!(
        matches!(&error, scion_server::Error::Corrupt(msg)
            if msg.contains("root_tree") && msg.contains("differs from the stored value")),
        "{error:?}"
    );
    // The refusal rolled back: the corrupted value still stands, and
    // the settle's other arms (the trees, the blob membership) wrote
    // nothing.
    let stored: Vec<u8> = client
        .query_one(
            "SELECT value FROM derived WHERE kind = 'root_tree' AND cs = $1",
            &[&cs.as_bytes().to_vec()],
        )
        .unwrap()
        .get(0);
    assert!(stored.ends_with(b"x"), "the corrupted value stands");
    let trees: i64 = client
        .query_one("SELECT count(*) FROM trees", &[])
        .unwrap()
        .get(0);
    assert_eq!(trees, 0, "the refused settle's tree arm rolled back");
}
