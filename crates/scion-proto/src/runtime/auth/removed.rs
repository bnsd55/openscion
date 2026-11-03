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

/// The argv forms of the secret flags that a binary has removed:
/// `(removed flag, what to use instead)`.
pub type RemovedSecretFlags = &'static [(&'static str, &'static str)];

/// Reads `argv` (the arguments after the program name) and answers the
/// error for the first removed secret flag it names, or `None` when the
/// call carries none. The scan stops at a bare `--`, after which every
/// argument is a positional value, not a flag. A flag matches by its
/// whole name: `--token` does not match `--token-file`, and the `=`
/// spelling (`--token=…`) matches the flag before its value.
///
/// The caller prints the error itself and exits, so the argument parser
/// never sees the line and cannot name the value in its own error.
#[must_use]
pub fn removed_secret_flag_error(
    argv: &[std::ffi::OsString],
    removed: &[(&'static str, &'static str)],
) -> Option<String> {
    let plain: Vec<String> = argv
        .iter()
        .map(|a| a.to_string_lossy().into_owned())
        .collect();
    for arg in &plain {
        if arg == "--" {
            return None;
        }
        for (flag, instead) in removed {
            if arg == flag
                || arg
                    .strip_prefix(flag)
                    .is_some_and(|rest| rest.starts_with('='))
            {
                return Some(format!(
                    "the {flag} flag is gone: a secret on the command line is readable \
                     by every local user (`ps`). {instead}"
                ));
            }
        }
    }
    None
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

    const REMOVED: &[(&str, &str)] = &[
        (
            "--token",
            "pass the token in SCION_TOKEN or --token-file FILE",
        ),
        (
            "--upstream-token",
            "pass the token in SCION_UPSTREAM_TOKEN or --upstream-token-file FILE",
        ),
    ];

    #[test]
    fn the_space_and_equals_spellings_both_refuse() {
        let error =
            removed_secret_flag_error(&argv(&["prog", "--token", "s3cr3t"], &[]), REMOVED).unwrap();
        assert!(error.contains("--token"), "{error}");
        assert!(error.contains("SCION_TOKEN"), "{error}");
        assert!(!error.contains("s3cr3t"), "{error}");
        let error =
            removed_secret_flag_error(&argv(&["prog", "--token=s3cr3t"], &[]), REMOVED).unwrap();
        assert!(error.contains("--token"), "{error}");
        assert!(!error.contains("s3cr3t"), "{error}");
    }

    #[test]
    fn a_lookalike_flag_is_not_the_removed_one() {
        assert_eq!(
            removed_secret_flag_error(&argv(&["prog", "--token-file", "f"], &[]), REMOVED),
            None
        );
        assert_eq!(
            removed_secret_flag_error(&argv(&["prog", "--tokens", "x"], &[]), REMOVED),
            None
        );
        assert_eq!(
            removed_secret_flag_error(&argv(&["prog", "--upstream-token-file", "f"], &[]), REMOVED),
            None
        );
    }

    #[test]
    fn the_upstream_flag_refuses_with_its_own_replacement() {
        let error =
            removed_secret_flag_error(&argv(&["prog", "--upstream-token", "s3cr3t"], &[]), REMOVED)
                .unwrap();
        assert!(error.contains("SCION_UPSTREAM_TOKEN"), "{error}");
        assert!(!error.contains("s3cr3t"), "{error}");
    }

    #[test]
    fn everything_after_a_bare_double_dash_is_a_value() {
        assert_eq!(
            removed_secret_flag_error(&argv(&["prog", "--", "--token", "s3cr3t"], &[]), REMOVED),
            None
        );
    }

    #[test]
    fn a_call_without_the_removed_flags_passes() {
        assert_eq!(
            removed_secret_flag_error(
                &argv(&["prog", "--server", "http://x", "checkout", "main"], &[]),
                REMOVED
            ),
            None
        );
    }
}
