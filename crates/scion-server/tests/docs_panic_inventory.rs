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
/// line, the enclosing function ("" for free-const sites), and the
/// token that matched.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct Site {
    file: String,
    line: usize,
    fn_name: String,
    token: String,
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
        // A `cfg(not(test))`/`cfg(not(any(test, ...)))` item is the
        // production twin: it stays in the sweep. Only a positive
        // `test` in the attribute's own predicate marks the item out;
        // matching the word `test` anywhere would blank the `not(test)`
        // twin and sweep a production fn as test code.
        let is_cfg =
            trimmed.starts_with("#[cfg(") && !trimmed.contains("not(") && trimmed.contains("test");
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
            let noisy = blank_noise(&src);
            let gated = mark_test_gated(&noisy);
            for (idx, line) in noisy.lines().enumerate() {
                if gated.contains(&idx) {
                    continue;
                }
                for token in TOKENS {
                    if line.contains(token) {
                        sites.push(Site {
                            file: rel.clone(),
                            line: idx + 1,
                            fn_name: enclosing_fn(&noisy, idx),
                            token: token.to_owned(),
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
            // The quoted panic text: the row may quote the token with its
            // message (`unreachable!("...")`) or name the shape
            // (`.expect("...")`); either appears as `token` plus backtick.
            row_text.contains(&site.token)
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
