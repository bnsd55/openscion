//! The field-fit boundary of the core's constructors: a field of
//! exactly `u32::MAX` bytes fits, one more is refused. cargo-mutants
//! skips every method named `new` (#1122), so the checks are pinned
//! here and planted by hand.

use scion_core::{Changeset, IdentityTime};

/// The field-fit boundary sits at `u32::MAX`: a field of exactly that many
/// bytes fits, one more is refused. The check reads only the length, so
/// the test fabricates an oversized vector without touching its pages
/// (a `set_len` on a vector whose allocation was never made at this
/// size): the test costs one page, not four gigabytes.
#[test]
fn a_field_of_u32_max_fits_and_one_more_is_refused() {
    // Each oversized field is its own calloc, built and dropped before
    // the next: the check reads only the length, so the vector's pages
    // are never read. Measured peak, test binary alone: 2.7 MB.
    // The panic must not format the value: its Debug would print every
    // byte of the 4 GiB vector and fill the disk (a plant run proved it).
    let Err(err) = IdentityTime::new(vec![0u8; u32::MAX as usize + 1], Vec::new(), 0, 0) else {
        // Unreachable: the refusal carries the reason, not the bytes.
        panic!("a name of u32::MAX + 1 bytes was accepted")
    };
    assert!(
        err.to_string().contains("identity name"),
        "the refusal must name the field: {err}"
    );
    let Err(err) = IdentityTime::new(Vec::new(), vec![0u8; u32::MAX as usize + 1], 0, 0) else {
        panic!("an email of u32::MAX + 1 bytes was accepted")
    };
    assert!(
        err.to_string().contains("identity email"),
        "the refusal must name the field: {err}"
    );
    let Err(err) = Changeset::new(
        Vec::new(),
        IdentityTime::new(Vec::new(), Vec::new(), 0, 0).unwrap(),
        None,
        vec![0u8; u32::MAX as usize + 1],
        std::collections::BTreeMap::new(),
    ) else {
        panic!("a message of u32::MAX + 1 bytes was accepted")
    };
    assert!(
        err.to_string().contains("message"),
        "the refusal must name the field: {err}"
    );

    // The fit side: a field of exactly u32::MAX bytes is accepted, and
    // the vector moves into the value - its pages are never read here
    // either. A `>` to `>=` flip in the fit check would refuse it.
    let name = vec![0u8; u32::MAX as usize];
    IdentityTime::new(name, vec![0u8; u32::MAX as usize], 0, 0).unwrap();
    let cs = Changeset::new(
        Vec::new(),
        IdentityTime::new(Vec::new(), Vec::new(), 0, 0).unwrap(),
        None,
        vec![0u8; u32::MAX as usize],
        std::collections::BTreeMap::new(),
    )
    .unwrap();
    assert_eq!(cs.message().len(), u32::MAX as usize);
}
