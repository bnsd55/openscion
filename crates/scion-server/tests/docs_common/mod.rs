//! Common to the suites that read the docs: the rules the docs'
//! checks share, so a link that renders on GitHub also resolves here.

/// GitHub's slug of one heading's text: punctuation drops, formatting
/// marks drop, each run of whitespace maps to one hyphen, and a hyphen
/// or an underscore in the heading stays. GitHub's own rule.
pub(crate) fn github_slug(text: &str) -> String {
    let mut out = String::new();
    let mut last_space = false;
    for c in text.chars() {
        if c.is_whitespace() {
            if !last_space {
                out.push('-');
            }
            last_space = true;
            continue;
        }
        last_space = false;
        if c == '`' || c == '*' || c == '_' {
            // Formatting is dropped, but a bare `_` or `*` word mark is
            // punctuation on GitHub too.
            continue;
        }
        if c.is_alphanumeric() || c == '-' || c == '_' {
            out.extend(c.to_lowercase());
        }
    }
    out
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_slug_follows_the_github_rules() {
        assert_eq!(super::github_slug("6. What to watch"), "6-what-to-watch");
        // Punctuation and formatting drop; digits, letters, `-` and `_` stay.
        // Each space maps to one hyphen, and a kept hyphen stays.
        assert_eq!(
            super::github_slug("The `Meta` trait (v1)"),
            "the-meta-trait-v1"
        );
        assert_eq!(
            super::github_slug("server --data <dir>"),
            "server---data-dir"
        );
        assert_eq!(
            super::github_slug("A stuck free-space sample"),
            "a-stuck-free-space-sample"
        );
        assert_eq!(super::github_slug("todo"), "todo");
    }
}
