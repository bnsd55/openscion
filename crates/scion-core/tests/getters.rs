//! The object model's fields read back as they were built: every getter
//! answers the exact value its constructor took, so a folded getter
//! cannot pass its value off as another.

use std::collections::BTreeMap;

use scion_core::{
    BlobId, Changeset, ChangesetId, Entry, FileEntry, FileState, GitOid, IdentityTime, PathElem,
    RepoPath, Tree,
};

/// Builds one identity and reads every field back exactly.
#[test]
fn an_identity_reads_back_its_fields() {
    let identity = IdentityTime::new(
        b"Grace".to_vec(),
        b"grace@example.com".to_vec(),
        1_700_000_123,
        120,
    )
    .expect("the identity fits the format");
    assert_eq!(identity.name(), b"Grace");
    assert_eq!(identity.email(), b"grace@example.com");
    assert_eq!(identity.unix_secs(), 1_700_000_123);
    assert_eq!(identity.tz_offset_minutes(), 120);
}

/// An object id displays as lowercase hex, as git prints it.
#[test]
fn an_object_id_displays_as_lowercase_hex() {
    let oid = GitOid::Sha1([0x0a; 20]);
    assert_eq!(oid.to_string(), "0a".repeat(20));
    assert_eq!(oid.as_bytes(), &[0x0a; 20]);
}

/// A changeset reads back its parents, its committer, its message and
/// its changes, each the exact value the constructor took.
#[test]
fn a_changeset_reads_back_its_fields() {
    let parent = ChangesetId::from_bytes([0x11; 32]);
    let author = IdentityTime::new(b"a".to_vec(), b"a@e".to_vec(), 1, 0)
        .expect("the identity fits the format");
    let committer = IdentityTime::new(b"c".to_vec(), b"c@e".to_vec(), 2, -30)
        .expect("the identity fits the format");
    let path = RepoPath::new(b"src/one").unwrap();
    let blob = BlobId::of(b"one\n");
    let mut changes = BTreeMap::new();
    changes.insert(
        path,
        Some(FileState::Regular {
            content: blob,
            executable: true,
        }),
    );
    let changeset = Changeset::new(
        vec![parent],
        author,
        Some(committer),
        b"the message".to_vec(),
        changes,
    )
    .expect("the changeset fits the format");
    assert_eq!(changeset.parents(), &[parent]);
    assert_eq!(changeset.author().name(), b"a");
    let committer = changeset.committer().expect("the committer is set");
    assert_eq!(committer.name(), b"c");
    assert_eq!(committer.tz_offset_minutes(), -30);
    assert_eq!(changeset.message(), b"the message");
    let changes = changeset.changes();
    assert_eq!(changes.len(), 1);
    let state = *changes.values().next().unwrap();
    assert_eq!(
        state.map(|s| s.blob()),
        Some(Some(blob)),
        "the change reads back as the regular entry it was"
    );
}

/// A file entry reads back its state and its size, and a tree reads
/// back its entries by name, whole and taken.
#[test]
fn an_entry_and_a_tree_read_back_their_fields() {
    let blob = BlobId::of(b"one\n");
    let entry = FileEntry::new(
        FileState::Regular {
            content: blob,
            executable: true,
        },
        5,
    );
    assert_eq!(
        FileState::Regular {
            content: blob,
            executable: true
        }
        .blob(),
        Some(blob),
        "a regular state names its blob"
    );
    assert_eq!(entry.size(), 5);
    let name = PathElem::new(b"one".to_vec()).unwrap();
    let tree = Tree::new(BTreeMap::from([(
        name.clone(),
        Entry::File(FileEntry::new(
            FileState::Regular {
                content: blob,
                executable: false,
            },
            4,
        )),
    )]))
    .expect("the tree fits the format");
    let entries = tree.entries();
    assert_eq!(entries.len(), 1);
    assert!(entries.contains_key(&name));
    let taken = tree.into_entries();
    assert_eq!(taken.len(), 1);
    assert!(taken.contains_key(&name));
}
