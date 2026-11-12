//! The key flag's hex parser, pinned at the unit level. The parser is
//! the daemon's front door for --ticket-public-key: a value that parses
//! wrong must be refused, never wrapped, and never a panic.

use scion_tier::key_from_hex;

#[test]
fn key_from_hex_takes_exactly_64_hex_characters() {
    let good = "a1".repeat(32);
    let bytes = key_from_hex(&good).expect("64 hex characters parse");
    assert_eq!(bytes, [0xa1; 32]);
    // Every nibble lands in its place: the high nibble is the even
    // character, the low one the odd.
    let mixed = "0123456789abcdef".repeat(4);
    assert_eq!(key_from_hex(&mixed).unwrap()[0], 0x01);
    assert_eq!(key_from_hex(&mixed).unwrap()[15], 0xef);

    // The input is trimmed first: a padded value still parses.
    assert_eq!(
        key_from_hex(&format!("  {0}  ", "ab".repeat(32))),
        Some([0xab; 32])
    );
    // The refusals: 63 and 65 characters, a non-hex byte, and an empty
    // string.
    assert_eq!(key_from_hex(&"a1".repeat(31)), None, "63 characters");
    assert_eq!(key_from_hex(&"a1".repeat(33)), None, "65 characters");
    assert_eq!(key_from_hex(&"zz".repeat(32)), None, "not hex");
    assert_eq!(key_from_hex(""), None, "empty");
}
