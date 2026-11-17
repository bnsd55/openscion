//! Direct checks of the request-field checks (`check`): the bounds the
//! server and every tier share, at each edge, both sides, with the
//! refusal's text.

use scion_proto::MAX_OBJECT_REFS;
use scion_proto::runtime::check::{self, MAX_NAME_BYTES};

#[test]
fn a_name_at_each_bound_answers_as_promised() {
    // 0 bytes: refused, naming the range.
    assert_eq!(
        check::name("repo", ""),
        Err("repo must be 1 to 256 bytes".into())
    );
    // 1 byte: the low edge is accepted.
    assert_eq!(check::name("repo", "a"), Ok(()));
    // MAX_NAME_BYTES bytes: the high edge is accepted.
    let at = "a".repeat(MAX_NAME_BYTES);
    assert_eq!(check::name("repo", &at), Ok(()));
    // One past: refused.
    let over = "a".repeat(MAX_NAME_BYTES + 1);
    assert_eq!(
        check::name("repo", &over),
        Err("repo must be 1 to 256 bytes".into())
    );
    // A control character is refused by its own rule, whatever the length.
    assert_eq!(
        check::name("repo", "a\nb"),
        Err("repo must not contain control characters".into())
    );
}

#[test]
fn an_object_count_at_each_bound_answers_as_promised() {
    assert!(check::object_count(0).is_err());
    assert!(check::object_count(1).is_ok());
    assert!(check::object_count(MAX_OBJECT_REFS).is_ok());
    assert!(check::object_count(MAX_OBJECT_REFS + 1).is_err());
    // The refusal is INVALID_ARGUMENT naming the range.
    let status = check::object_count(MAX_OBJECT_REFS + 1).unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert!(status.message().contains(&MAX_OBJECT_REFS.to_string()));
}
