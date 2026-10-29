//! The batch reads answer every id they are asked, however often it
//! appears: two merges can share a side parent, and one tree id can sit
//! under two paths, so a batch may name the same id twice. The reply
//! must carry one entry per ASKED id (the meta.rs contract), not one
//! entry per distinct row.

#![cfg(unix)]

mod common;

use scion_core::{BlobId, Changeset, FileState, IdentityTime, RepoPath};
use scion_server::Backend;
use scion_server::test_support::meta::{Meta as _, MetaTx as _};

#[test]
fn a_batch_that_asks_an_id_twice_answers_it_twice() {
    let dir = scion_server::test_support::private_tempdir();
    let store = Backend::open_test(dir.path()).unwrap();
    store
        .write(|c| {
            let repo = c.create_repo("r")?;
            // One tree row, asked twice in one batch. The body is a real
            // tree: put_trees decodes and checks what it stores.
            let empty = scion_core::Tree::new([].into()).expect("an empty tree fits the format");
            let (body, tree) = empty.encode_with_id();
            c.put_trees(&[(tree, body.clone())])?;
            let trees = c.get_trees(&[tree, tree])?;
            assert_eq!(
                trees,
                vec![Some(body.clone()), Some(body)],
                "get_trees must answer every asked id"
            );
            // One changeset with a derived row, asked twice in one batch.
            let state = FileState::Regular {
                content: BlobId::of(b"f0"),
                executable: false,
            };
            let cs = Changeset::new(
                vec![],
                IdentityTime::new(b"a".to_vec(), b"a@x".to_vec(), 0, 0)
                    .expect("identity fits the format"),
                None,
                b"f0".to_vec(),
                [(RepoPath::new("f0").unwrap(), Some(state))].into(),
            )
            .expect("changeset fits the format");
            let cs = c.insert_changeset(repo, &cs)?;
            c.put_derived("commit graph", 1, cs, b"value")?;
            let got = c.get_derived_batch("commit graph", 1, &[cs, cs])?;
            assert_eq!(
                got,
                vec![Some(b"value".to_vec()), Some(b"value".to_vec())],
                "get_derived_batch must answer every asked id"
            );
            Ok(())
        })
        .unwrap();
}
