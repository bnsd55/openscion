//! Parsing an object ID from text and bytes, at each edge: the hex
//! length, the byte length, and the parse error's text. These are not
//! contract-golden material - they live outside `tests/golden.rs` so the
//! pairing gate stays silent.

use scion_core::ChangesetId;

/// Any well-formed ID, parsed from its own hex form.
fn an_id() -> ChangesetId {
    ChangesetId::from_bytes([7; 32])
        .to_string()
        .parse::<ChangesetId>()
        .unwrap()
}

#[test]
fn a_hex_id_parses_at_exactly_64_digits() {
    let id = an_id();
    assert_eq!(id.to_string().parse::<ChangesetId>(), Ok(id));
}

#[test]
fn a_hex_id_off_the_length_is_refused_whole() {
    // A padding or truncating parser would take these.
    let sixty_three = "a".repeat(63);
    assert!(sixty_three.parse::<ChangesetId>().is_err());
    let sixty_five = "a".repeat(65);
    assert!(sixty_five.parse::<ChangesetId>().is_err());
}

#[test]
fn a_byte_slice_off_32_is_refused_whole() {
    assert!(ChangesetId::try_from([0u8; 31].as_slice()).is_err());
    // A first-32-bytes parser would take this.
    assert!(ChangesetId::try_from([0u8; 33].as_slice()).is_err());
}

#[test]
fn the_parse_error_names_its_rule() {
    assert_eq!(
        "abc".parse::<ChangesetId>().unwrap_err().to_string(),
        "not an object ID: expected 64 hex digits or 32 bytes"
    );
}
