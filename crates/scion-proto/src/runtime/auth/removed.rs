//! The argv forms of secret flags are gone: a command line is readable
//! by every local user (`ps`, `/proc/<pid>/cmdline`), and a token that
//! arrived there was a secret from no one. The file form (a checked
//! secret read) and the environment form remain.
//!
//! A call that still passes a removed flag fails at once, before any
//! argument parsing, with an error that names the forms to use
//! instead. The value the old flag carried is never echoed — not by
//! this check, and not by the argument parser behind it, which never
//! runs for a removed flag.

/// One removed argv flag: its whole name, and the forms to use instead.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RemovedFlag {
    /// The flag's whole name, `--token` style.
    pub flag: &'static str,
    /// The forms that replace it, as the refusal names them.
    pub instead: &'static str,
}

/// The removed flags a binary refuses: a static table.
pub type RemovedFlags = &'static [RemovedFlag];

/// Reads `argv` (the arguments after the program name) and answers the
/// first removed flag it names, or `None` when the call carries none.
/// The scan stops at a bare `--`, after which every argument is a
/// positional value, not a flag. A flag matches by its whole name:
/// `--token` does not match `--token-file`, and the `=` spelling
/// (`--token=…`) matches the flag before its value.
///
/// The caller formats the refusal itself and exits, so the argument
/// parser never sees the line and cannot name the value in its own
/// error.
#[must_use]
pub fn removed_flag_of(argv: &[std::ffi::OsString], removed: RemovedFlags) -> Option<RemovedFlag> {
    let plain: Vec<String> = argv
        .iter()
        .map(|a| a.to_string_lossy().into_owned())
        .collect();
    for arg in &plain {
        if arg == "--" {
            return None;
        }
        for removed in removed {
            if arg == removed.flag
                || arg
                    .strip_prefix(removed.flag)
                    .is_some_and(|rest| rest.starts_with('='))
            {
                return Some(*removed);
            }
        }
    }
    None
}

/// The refusal a binary prints for a removed flag. Built only from the
/// static table's own words: the value the caller passed is never part
/// of it.
#[must_use]
pub fn refusal_text(removed: RemovedFlag) -> String {
    format!(
        "the {0} flag is gone: a secret on the command line is readable by every local user \
         (`ps`). {1}",
        removed.flag, removed.instead
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn argv(before: &[&str], rest: &[&str]) -> Vec<std::ffi::OsString> {
        let mut v: Vec<std::ffi::OsString> = Vec::new();
        for a in before {
            v.push(a.into());
        }
        for a in rest {
            v.push(a.into());
        }
        v
    }

    const REMOVED: RemovedFlags = &[
        RemovedFlag {
            flag: "--token",
            instead: "pass the token in SCION_TOKEN or --token-file FILE",
        },
        RemovedFlag {
            flag: "--upstream-token",
            instead: "pass the token in SCION_UPSTREAM_TOKEN or --upstream-token-file FILE",
        },
    ];

    #[test]
    fn the_space_and_equals_spellings_both_refuse() {
        let text = refusal_text(
            removed_flag_of(&argv(&["prog", "--token", "s3cr3t"], &[]), REMOVED).unwrap(),
        );
        assert!(text.contains("--token"), "{text}");
        assert!(text.contains("SCION_TOKEN"), "{text}");
        assert!(!text.contains("s3cr3t"), "{text}");
        let text = refusal_text(
            removed_flag_of(&argv(&["prog", "--token=s3cr3t"], &[]), REMOVED).unwrap(),
        );
        assert!(text.contains("--token"), "{text}");
        assert!(!text.contains("s3cr3t"), "{text}");
    }

    #[test]
    fn a_lookalike_flag_is_not_the_removed_one() {
        assert_eq!(
            removed_flag_of(&argv(&["prog", "--token-file", "f"], &[]), REMOVED),
            None
        );
        assert_eq!(
            removed_flag_of(&argv(&["prog", "--tokens", "x"], &[]), REMOVED),
            None
        );
        assert_eq!(
            removed_flag_of(&argv(&["prog", "--upstream-token-file", "f"], &[]), REMOVED),
            None
        );
    }

    #[test]
    fn the_upstream_flag_refuses_with_its_own_replacement() {
        let text = refusal_text(
            removed_flag_of(&argv(&["prog", "--upstream-token", "s3cr3t"], &[]), REMOVED).unwrap(),
        );
        assert!(text.contains("SCION_UPSTREAM_TOKEN"), "{text}");
        assert!(!text.contains("s3cr3t"), "{text}");
    }

    #[test]
    fn everything_after_a_bare_double_dash_is_a_value() {
        assert_eq!(
            removed_flag_of(&argv(&["prog", "--", "--token", "s3cr3t"], &[]), REMOVED),
            None
        );
    }

    #[test]
    fn a_call_without_the_removed_flags_passes() {
        assert_eq!(
            removed_flag_of(
                &argv(&["prog", "--server", "http://x", "checkout", "main"], &[]),
                REMOVED
            ),
            None
        );
    }
}
