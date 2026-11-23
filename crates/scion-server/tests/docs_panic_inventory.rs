//! The panic inventory in section 5a of `docs/production-audit.md`
//! must name every panic site (`unwrap`, `expect`, `panic!`,
//! `unreachable!`, `todo!`, `unimplemented!`) in the non-test code of
//! `scion-server`, `scn`, `scion-proto` and `scion-tier`, and every row
//! must name a live site. The sweep runs here, on the working tree, so
//! a merge that adds a panic site without a row fails CI, and a rename
//! that orphans a row fails it the other way (#1402).

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

const ROOT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");
const AUDIT: &str = "docs/production-audit.md";
const SECTION: &str = "### 5a. The panic inventory";
const NEXT_SECTION: &str = "### 5b. The lock-poison inventory";
const CRATES: [&str; 4] = ["scion-server", "scn", "scion-proto", "scion-tier"];

/// The panic tokens, as they appear in code. `.expect(` and `.unwrap()`
/// keep the leading dot so an identifier ending in `expect` cannot
/// match; the macros are word-matched.
const TOKENS: [&str; 6] = [
    ".unwrap()",
    ".expect(",
    "panic!(",
    "unreachable!(",
    "todo!(",
    "unimplemented!(",
];

/// A site the sweep found: file (`scion-server/src/main.rs`), 1-based
/// line, the enclosing function ("" for free-const sites), the token
/// that matched, and the message the site's own string literal carries
/// (`""` for `unwrap` and for macros; `expect` always names one).
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct Site {
    file: String,
    line: usize,
    fn_name: String,
    token: String,
    message: String,
}

/// Blanks comments and the contents of string and char literals, line
/// and length preserving: braces and tokens inside them can no longer
/// mislead the brace counter or match a token.
#[allow(clippy::too_many_lines)]
fn blank_noise(src: &str) -> String {
    let chars: Vec<char> = src.chars().collect();
    let mut out = chars.clone();
    let n = chars.len();
    let mut i = 0;
    let (mut in_str, mut in_char, mut in_line, mut in_block) = (false, false, false, false);
    while i < n {
        let c = chars[i];
        let nxt = if i + 1 < n { chars[i + 1] } else { ' ' };
        if in_line {
            if c == '\n' {
                in_line = false;
            } else {
                out[i] = ' ';
            }
            i += 1;
            continue;
        }
        if in_block {
            if c == '*' && nxt == '/' {
                in_block = false;
                out[i] = ' ';
                out[i + 1] = ' ';
                i += 2;
            } else {
                if c != '\n' {
                    out[i] = ' ';
                }
                i += 1;
            }
            continue;
        }
        if in_str {
            match c {
                '\\' => {
                    out[i] = ' ';
                    if i + 1 < n && chars[i + 1] != '\n' {
                        out[i + 1] = ' ';
                    }
                    i += 2;
                }
                '"' => {
                    in_str = false;
                    i += 1;
                }
                '\n' => i += 1,
                _ => {
                    out[i] = ' ';
                    i += 1;
                }
            }
            continue;
        }
        if in_char {
            match c {
                '\\' => {
                    out[i] = ' ';
                    if i + 1 < n && chars[i + 1] != '\n' {
                        out[i + 1] = ' ';
                    }
                    i += 2;
                }
                '\'' => {
                    in_char = false;
                    i += 1;
                }
                _ => {
                    out[i] = ' ';
                    i += 1;
                }
            }
            continue;
        }
        if c == '/' && nxt == '/' {
            in_line = true;
            out[i] = ' ';
            out[i + 1] = ' ';
            i += 2;
            continue;
        }
        if c == '/' && nxt == '*' {
            in_block = true;
            out[i] = ' ';
            out[i + 1] = ' ';
            i += 2;
            continue;
        }
        if c == '"' {
            in_str = true;
            out[i] = '"';
            i += 1;
            continue;
        }
        if c == 'r' && (nxt == '"' || (nxt == '#' && chars[i..].contains(&'"'))) {
            // A raw string: `r"..."`, `r#"..."#`, `r##"..."##`, ... It
            // closes only at the quote followed by the same number of
            // hashes, so an inner quote or an inner `"#` cannot close it.
            let mut hashes = 0usize;
            let mut scan = i + 1;
            while scan < n && chars[scan] == '#' {
                hashes += 1;
                scan += 1;
            }
            if scan < n && chars[scan] == '"' {
                // Walk to the closing quote-plus-hashes; keep the newlines.
                let mut at = scan + 1;
                while at < n {
                    if chars[at] == '"' {
                        let mut close = at + 1;
                        while close < n && chars[close] == '#' {
                            close += 1;
                        }
                        if close - at - 1 == hashes {
                            break;
                        }
                        at = close;
                    } else {
                        at += 1;
                    }
                }
                // Blank the whole literal, quotes and hashes included,
                // except its newlines.
                for blanked in i..=at.min(n - 1) {
                    if chars[blanked] != '\n' {
                        out[blanked] = ' ';
                    }
                }
                i = at + 1;
                continue;
            }
            // Not a raw string opening: an identifier that happens to be
            // `r` (a lifetime-name clash is impossible in code position);
            // fall through as a plain character.
            out[i] = 'r';
            i += 1;
            continue;
        }
        if c == '\'' {
            // A char literal is at most an escape or one character wide.
            let lit = chars[i..].len() >= 3
                && (chars[i + 1] == '\\' && i + 3 < n && chars[i + 2] != '\''
                    || (chars[i + 1] != '\\' && i + 2 < n && chars[i + 2] == '\''));
            if lit {
                in_char = true;
                i += 1;
                continue;
            }
        }
        out[i] = c;
        i += 1;
    }
    out.into_iter().collect()
}

/// Whether one `cfg(...)` predicate (from its opening paren to its
/// matching close) gates an item out of non-test builds: a positive
/// `test` word or the exact feature `test-hooks`, with no `not(...)`
/// around either. `cfg(not(test))` stays in; a feature named
/// `latest-thing` is not `test-hooks`, whatever it contains.
fn predicate_gates_tests(part: &str) -> bool {
    let Some(open) = part.find('(') else {
        return false;
    };
    // The predicate's own span: to the paren that balances the opener.
    let mut depth = 0usize;
    let mut end = part.len();
    for (offset, c) in part[open..].char_indices() {
        match c {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    end = open + offset + 1;
                    break;
                }
            }
            _ => {}
        }
    }
    let predicate = &part[open..end];
    let not_wrapped = predicate.contains("not(");
    let test_word = predicate
        .split(|c: char| !c.is_alphanumeric() && c != '_')
        .any(|word| word == "test");
    let test_hooks = predicate.contains("feature = \"test-hooks\"");
    (test_word || test_hooks) && !not_wrapped
}

/// Marks the 0-based line indexes of items gated out of non-test
/// builds: any `cfg(...)` attribute whose predicate mentions `test`
/// (as `cfg(test)` or `cfg(any(test, feature = "test-hooks"))`). The
/// item spans its attribute block through the end of its body, found
/// by exact brace matching on the noise-blanked text.
fn mark_test_gated(noisy: &str) -> BTreeSet<usize> {
    let lines: Vec<&str> = noisy.lines().collect();
    let mut marked = BTreeSet::new();
    let line_count = lines.len();
    let mut at = 0;
    while at < line_count {
        let trimmed = lines[at].trim_start();
        // An attribute list may share one line (`#[allow(dead_code)]
        // #[cfg(test)]`), so the cfg can sit anywhere in the line's
        // attribute run - but each `cfg(...)` predicate decides alone. A
        // `cfg(not(test))`/`cfg(not(any(test, ...)))` item is the
        // production twin: it stays in the sweep. The predicate matches
        // the word `test` exactly or the feature `test-hooks` exactly, so
        // a future feature named `latest-thing` cannot gate a production
        // item out by sharing a substring.
        let is_cfg = trimmed.contains("#[cfg(")
            && trimmed
                .split("#[")
                .filter(|part| part.starts_with("cfg("))
                .any(predicate_gates_tests);
        if is_cfg {
            // The attribute may span lines; find its closing bracket.
            let mut attr_end = at;
            while attr_end < line_count && !lines[attr_end].trim_end().ends_with(']') {
                attr_end += 1;
            }
            // Skip any further attribute lines of the same item.
            let mut body_at = attr_end + 1;
            while body_at < line_count && lines[body_at].trim_start().starts_with("#[") {
                while body_at < line_count && !lines[body_at].trim_end().ends_with(']') {
                    body_at += 1;
                }
                body_at += 1;
            }
            // The item: `{ ... }` or a `;`-ended declaration.
            let (mut depth, mut seen) = (0i64, false);
            let mut end = body_at;
            while body_at < line_count {
                let line = lines[body_at];
                #[allow(clippy::cast_possible_wrap)]
                {
                    depth += line.matches('{').count() as i64 - line.matches('}').count() as i64;
                }
                if line.contains('{') {
                    seen = true;
                }
                if seen && depth <= 0 {
                    end = body_at;
                    break;
                }
                if !seen && line.contains(';') {
                    end = body_at;
                    break;
                }
                body_at += 1;
            }
            let last = end.min(line_count - 1);
            for marked_at in at..=last {
                marked.insert(marked_at);
            }
            at = end + 1;
            continue;
        }
        at += 1;
    }
    marked
}

/// The nearest enclosing `fn` name of a line, by brace walking the
/// noise-blanked text (exact: braces inside literals are blanked).
fn enclosing_fn(noisy: &str, target: usize) -> String {
    let mut stack: Vec<Option<String>> = Vec::new();
    let mut pending: Option<String> = None;
    for (ln, line) in noisy.lines().enumerate() {
        if ln >= target {
            break;
        }
        let bytes = line.as_bytes();
        let mut k = 0;
        while k < bytes.len() {
            if line[k..].starts_with("fn ") {
                let rest = &line[k + 3..];
                let name: String = rest
                    .chars()
                    .take_while(|c| c.is_alphanumeric() || *c == '_')
                    .collect();
                if !name.is_empty() {
                    pending = Some(name);
                }
                k += 3;
                continue;
            }
            match bytes[k] {
                b'{' => {
                    stack.push(pending.take());
                    k += 1;
                }
                b'}' => {
                    stack.pop();
                    k += 1;
                }
                _ => k += 1,
            }
        }
    }
    stack.into_iter().rev().flatten().next().unwrap_or_default()
}

/// Every panic site in the non-test code of the four crates: file
/// (`scion-server/src/main.rs`), line, function, token.
fn sweep() -> Vec<Site> {
    let mut sites = Vec::new();
    for crate_name in CRATES {
        let base = Path::new(ROOT).join("crates").join(crate_name).join("src");
        for path in rust_files(&base) {
            let rel = path
                .to_string_lossy()
                .replace('\\', "/")
                .split("/crates/")
                .last()
                .unwrap()
                .to_owned();
            if rel.contains("test_support") {
                // 5a's own line holds test-only helpers outside the audit.
                continue;
            }
            let src = fs::read_to_string(&path).unwrap();
            let src_lines: Vec<&str> = src.lines().collect();
            let noisy = blank_noise(&src);
            let gated = mark_test_gated(&noisy);
            for (idx, line) in noisy.lines().enumerate() {
                if gated.contains(&idx) {
                    continue;
                }
                for token in TOKENS {
                    if line.contains(token) {
                        let message = site_message(src_lines[idx], token);
                        sites.push(Site {
                            file: rel.clone(),
                            line: idx + 1,
                            fn_name: enclosing_fn(&noisy, idx),
                            token: token.to_owned(),
                            message,
                        });
                    }
                }
            }
        }
    }
    sites.sort();
    sites
}

/// Every `.rs` file under `dir`, any depth; `tests`, `benches` and
/// `fuzz` directories are skipped (they are separate targets, never
/// `src` modules).
fn rust_files(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if name != "tests" && name != "benches" && name != "fuzz" {
                    stack.push(path);
                }
            } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
                out.push(path);
            }
        }
    }
    out
}

/// The 5a rows: the lines between the section's header and its next
/// heading that start with a pipe, with their three cells split on the
/// pipes outside backticks.
fn rows_5a(text: &str) -> Vec<(String, Vec<String>)> {
    let section = text.split(SECTION).nth(1).expect("5a is present");
    let section = section.split(NEXT_SECTION).next().expect("5b follows 5a");
    let mut rows = Vec::new();
    for line in section.lines() {
        if !line.starts_with('|') || line.starts_with("| Site |") || line.starts_with("| ---") {
            continue;
        }
        let mut cells = Vec::new();
        let mut cell = String::new();
        let mut in_ticks = false;
        for ch in line.chars() {
            match ch {
                '`' => {
                    in_ticks = !in_ticks;
                    cell.push(ch);
                }
                '|' if !in_ticks => {
                    cells.push(cell.trim().to_owned());
                    cell.clear();
                }
                _ => cell.push(ch),
            }
        }
        if !cell.is_empty() {
            cells.push(cell.trim().to_owned());
        }
        // cells[0] is the empty text before the first pipe.
        if cells.len() >= 2 {
            rows.push((cells[1].clone(), cells));
        }
    }
    rows
}

/// The verdict word of a row's verdict cell, read before the dash. The
/// reverse test skips only the two verdict words that say the row
/// records no live panic site on purpose.
fn skip_word(verdict: &str) -> bool {
    let word = verdict.split('—').next().unwrap_or(verdict).trim();
    word == "FIXED" || word == "NOT A PANIC"
}

/// The backticked tokens of a row's site cell.
fn backticked(cell: &str) -> Vec<String> {
    cell.split('`')
        .skip(1)
        .step_by(2)
        .map(str::to_owned)
        .collect()
}

/// The message of a panic site, read from the original source line:
/// the string literal inside the call, for `expect` and for the macros
/// (`panic!("...")` and so on). `unwrap` carries no message; the answer
/// is `""`, and no row can match it by message.
fn site_message(line: &str, token: &str) -> String {
    let Some(at) = line.find(token) else {
        return String::new();
    };
    let rest = &line[at + token.len()..];
    let rest = rest.trim_start();
    // `.expect(` / `panic!(` / ... is followed by the message's opening
    // quote; anything else (a `?`, a `.`, a `,`) means no message here.
    let Some(quote_at) = rest.find('"') else {
        return String::new();
    };
    let after_quote = &rest[quote_at + 1..];
    let mut message = String::new();
    let mut chars = after_quote.chars();
    while let Some(c) = chars.next() {
        match c {
            '\\' => {
                // An escaped char rides as-is: `\"` does not close.
                if let Some(escaped) = chars.next() {
                    message.push('\\');
                    message.push(escaped);
                }
            }
            '"' => return message,
            _ => message.push(c),
        }
    }
    message
}

/// A row names a site when its file cell names the site's file (a full
/// path, or the file's basename uniquely across the four crates) AND
/// the site's function or quoted token text appears in the row.
fn row_names_site(
    row_cells: &[String],
    file_cell: &str,
    site: &Site,
    all_files: &[String],
) -> bool {
    let names_file = backticked(file_cell).iter().any(|token| {
        let token = token.strip_prefix("crates/").unwrap_or(token);
        if token.contains("/src/") {
            site.file == token || site.file.ends_with(&format!("/{token}"))
        } else if token.to_ascii_lowercase().ends_with(".rs") {
            // The token came out of backticks in a Markdown cell; the docs
            // write the extension lowercase, so the tail compare is safe.
            // A bare basename must be unique across the crates' src trees.
            let matches = all_files
                .iter()
                .filter(|f| f.rsplit('/').next() == Some(token))
                .count();
            matches == 1 && site.file.rsplit('/').next() == Some(token)
        } else {
            false
        }
    });
    if !names_file {
        return false;
    }
    let row_text = row_cells.join(" ");
    if site.fn_name.is_empty() {
        return true; // a free-const site: the file names it
    }
    // The row names the fn: a `fn \`name\`` anchor, one name in the row's
    // backticked fn list (`fn \`a\`, \`b\``), or the name as the tail of a
    // pathed token (`Limits::default`, `Paint::next`). The file cell already
    // pins the file, so a backticked token that ends in the fn's name is the
    // fn reference.
    row_text.contains(&format!("fn `{}`", site.fn_name))
        || backticked(&row_text).iter().any(|token| {
            token == &site.fn_name
                || token
                    .rsplit("::")
                    .next()
                    .is_some_and(|tail| tail == site.fn_name)
        })
        || {
            // The quoted panic text: the row claims the site only when it
            // quotes the site's OWN message (`unreachable!("the loop
            // returns at every entry")`). A row that quotes one site's
            // message must not claim every site of the same token shape in
            // its file - the message pins which site the row speaks of.
            !site.message.is_empty() && row_text.contains(&format!("\"{}\"", site.message))
        }
}

#[test]
fn every_panic_site_has_a_5a_row() {
    let text = fs::read_to_string(Path::new(ROOT).join(AUDIT)).expect("the audit file opens");
    let rows = rows_5a(&text);
    let sites = sweep();
    let all_files: Vec<String> = sites.iter().map(|s| s.file.clone()).collect();
    let unclaimed: Vec<&Site> = sites
        .iter()
        .filter(|site| {
            !rows
                .iter()
                .any(|(file_cell, cells)| row_names_site(cells, file_cell, site, &all_files))
        })
        .collect();
    assert!(
        unclaimed.is_empty(),
        "{AUDIT} 5a must name every panic site in non-test code; these have no row:\n{}",
        unclaimed
            .iter()
            .map(|s| format!(
                "{}:{} fn `{}` token `{}`",
                s.file, s.line, s.fn_name, s.token
            ))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

#[test]
fn every_5a_row_names_a_live_site() {
    let text = fs::read_to_string(Path::new(ROOT).join(AUDIT)).expect("the audit file opens");
    let rows = rows_5a(&text);
    let sites = sweep();
    let all_files: Vec<String> = sites.iter().map(|s| s.file.clone()).collect();
    let stale: Vec<String> = rows
        .iter()
        .filter(|(_, cells)| !skip_word(&cells[cells.len() - 1]))
        .filter(|(file_cell, cells)| {
            !sites
                .iter()
                .any(|site| row_names_site(cells, file_cell, site, &all_files))
        })
        .map(|(file_cell, _)| file_cell.clone())
        .collect();
    assert!(
        stale.is_empty(),
        "{AUDIT} 5a rows must each name a live panic site; these name none \
         (a rename moved the code: update the row; a verdict that records no \
         live site on purpose reads FIXED or NOT A PANIC):\n{}",
        stale.join("\n")
    );
}

#[test]
fn blank_noise_holds_raw_strings_and_shared_line_attributes() {
    // A raw string whose contents would otherwise move the fn walker
    // (a `}`) and add a false site (an `.unwrap()`): F2's fixture.
    let raw = concat!(
        "fn outer() -> usize {\n",
        "    let text = r#\"a } brace and .unwrap() inside\"#;\n",
        "    text.len()\n",
        "}\n"
    );
    let noisy = blank_noise(raw);
    let sites: Vec<&str> = TOKENS
        .iter()
        .filter(|token| noisy.contains(**token))
        .copied()
        .collect();
    assert!(
        sites.is_empty(),
        "a raw string's contents must read as string, not code: {sites:?}"
    );
    // The fn walker sees one fn with one brace pair, no phantom close.
    assert_eq!(enclosing_fn(&noisy, 2), "outer");
    // A hash-quoted raw string with an inner plain quote: F2's second
    // shape. It closes only at quote-plus-hash, so the inner quote stays
    // inside the literal.
    let hashed = concat!(
        "fn middle() {\n",
        "    let text = r#\"an inner \" quote and .expect(\u{22}x\u{22}) stay\"#;\n",
        "}\n"
    );
    let noisy = blank_noise(hashed);
    assert!(
        !noisy.contains(".expect("),
        "a raw string's inner quote must not end the literal early"
    );
    assert_eq!(enclosing_fn(&noisy, 2), "middle");
    // Two attributes on one line: F3's fixture. The cfg marks the item
    // out even though the line starts with another attribute.
    let shared = "#[allow(dead_code)] #[cfg(test)]\nfn hidden() {}\n";
    let marked = mark_test_gated(shared);
    // The attribute line and the fn line (the item closes on it); a
    // token search on either must be skipped.
    assert_eq!(marked.len(), 2, "the shared-line cfg marks its item out");
    // A feature whose name merely contains "test" gates nothing out: the
    // predicate must name `test` or `test-hooks` exactly.
    let near = "#[cfg(feature = \"latest-thing\")]\nfn real() {}\n";
    assert!(mark_test_gated(near).is_empty());
    // `not(test)` stays in.
    let twin = "#[cfg(not(test))]\nfn real() {}\n";
    assert!(mark_test_gated(twin).is_empty());
}

#[test]
fn no_todo_or_unimplemented_in_production() {
    let sites = sweep();
    let offenders: Vec<&Site> = sites
        .iter()
        .filter(|s| s.token == "todo!(" || s.token == "unimplemented!(")
        .collect();
    assert!(
        offenders.is_empty(),
        "{AUDIT} 5a promises no todo!/unimplemented! in production code; found:\n{}",
        offenders
            .iter()
            .map(|s| format!("{}:{} fn `{}`", s.file, s.line, s.fn_name))
            .collect::<Vec<_>>()
            .join("\n")
    );
}
