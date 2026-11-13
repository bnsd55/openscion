//! The mutation-testing table in section 10 of `docs/production-audit.md`
//! must stay one Markdown table: a blank line between its rows ends the
//! table, and GitHub then renders every later row as plain text with
//! pipes (#1130). The test walks the section's table from its header to
//! its last row and refuses any blank line in between, naming the line.

use std::fs;
use std::path::{Path, PathBuf};

const ROOT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");
const AUDIT: &str = "docs/production-audit.md";
const SECTION: &str = "## 10. Test strength (mutation testing)";
const HEADER: &str = "| Module (the mutants' run) | Mutants | What survived, and what killed it |";

#[test]
fn the_mutation_table_has_no_blank_line_between_its_rows() {
    let text = fs::read_to_string(Path::new(ROOT).join(AUDIT)).expect("the audit file exists");
    let lines: Vec<&str> = text.lines().collect();
    let section = lines
        .iter()
        .position(|line| *line == SECTION)
        .unwrap_or_else(|| panic!("{AUDIT} lost its section 10 heading"));
    let header = lines[section..]
        .iter()
        .position(|line| *line == HEADER)
        .unwrap_or_else(|| panic!("{AUDIT}'s section 10 lost its table header"))
        + section;
    // The table's last row: the last line of section 10 that starts with
    // a pipe (the next `## ` heading ends the section).
    let section_end = match lines[header..]
        .iter()
        .position(|line| line.starts_with("## "))
    {
        Some(at) => at + header,
        None => lines.len(),
    };
    let last_row = lines[header..section_end]
        .iter()
        .rposition(|line| line.starts_with('|'))
        .unwrap_or_else(|| panic!("{AUDIT}'s section 10 table lost its rows"))
        + header;
    let blanks: Vec<usize> = lines[header..=last_row]
        .iter()
        .enumerate()
        .filter(|(_, line)| line.trim().is_empty())
        .map(|(at, _)| at + header + 1)
        .collect();
    assert!(
        blanks.is_empty(),
        "{AUDIT}: a blank line ends a Markdown table, so these rows \
         between lines {} and {} render as plain text on GitHub: {:?}; \
         join the rows into one block",
        header + 1,
        last_row + 1,
        blanks
    );
}

/// The `.rs` paths a section-10 row names, normalized: a leading
/// `crates/` is stripped, so `crates/scn/src/vfs.rs` and `scn/src/vfs.rs`
/// name the same file.
fn row_paths(section: &str) -> Vec<String> {
    let mut paths = Vec::new();
    for line in section.lines() {
        if !line.starts_with('|') {
            continue;
        }
        let mut rest = line;
        while let Some(open) = rest.find('`') {
            let after = &rest[open + 1..];
            let Some(close) = after.find('`') else { break };
            let token = &after[..close];
            if token.len() > 3 && token[token.len() - 3..].eq_ignore_ascii_case(".rs") {
                paths.push(token.strip_prefix("crates/").unwrap_or(token).to_owned());
            }
            rest = &after[close + 1..];
        }
    }
    paths
}

/// Every `crates/*/src/**/*.rs` file, as `crate/src/rest`.
fn source_files() -> Vec<String> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        for entry in fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                if path.file_name().is_some_and(|n| n == "target") {
                    continue;
                }
                walk(&path, out);
            } else if path.extension().is_some_and(|e| e == "rs")
                && path.components().any(|c| c.as_os_str() == "src")
            {
                out.push(path);
            }
        }
    }
    let mut out = Vec::new();
    walk(&Path::new(ROOT).join("crates"), &mut out);
    out.sort();
    out.into_iter()
        .filter_map(|p| {
            let s = p.to_string_lossy().replace('\\', "/");
            s.split("/crates/").last().map(str::to_owned)
        })
        .collect()
}

/// A row names a file when its normalized token ends with the file's
/// `src/...` path, or when the token is a bare basename that is unique
/// across the source files (a bare basename naming two files resolves
/// nothing, so its rows must carry fuller paths).
fn names_file(paths: &[String], file: &str, all: &[String]) -> bool {
    let Some((_, rel)) = file.split_once("/src/") else {
        return false;
    };
    for p in paths {
        if p.contains("/src/") {
            // A token that keeps `src/` is a full path: it names the
            // file when the file ends with it at a path boundary.
            if file == p.as_str() || file.ends_with(&format!("/{p}")) {
                return true;
            }
        } else if p.contains('/') {
            // A src-less token like `runtime/metrics.rs` is the file's
            // after-`src/` path and must equal it exactly: a tail match
            // would let `runtime/metrics.rs` claim
            // scion-server/src/metrics.rs too.
            if rel == p.as_str() {
                return true;
            }
        } else {
            // A bare basename names the file only when it is the file's
            // own basename and that basename is unique across all the
            // source files.
            if rel.rsplit('/').next() == Some(p.as_str()) {
                let unique = all
                    .iter()
                    .filter(|f| f.rsplit('/').next() == Some(p.as_str()))
                    .count()
                    == 1;
                if unique {
                    return true;
                }
            }
        }
    }
    false
}

/// The excluded files, parsed from the audit's "### The files no row
/// names" block: the single source. Every backticked `crates/...` path
/// in the block is an exclusion; the entry's text before the colon is
/// its reason. The parser refuses a block path that names no source
/// file, so a renamed or deleted file cannot sit in the list.
fn excluded_from_audit(text: &str) -> Vec<String> {
    let block = text
        .split("### The files no row names")
        .nth(1)
        .unwrap_or_else(|| panic!("{AUDIT} lost its exclusion block"));
    let block = block.split("\n## ").next().unwrap();
    let mut excluded = Vec::new();
    for line in block.lines() {
        let mut rest = line;
        while let Some(open) = rest.find('`') {
            let after = &rest[open + 1..];
            let Some(close) = after.find('`') else { break };
            let token = &after[..close];
            if token.starts_with("crates/")
                && token.len() > 3
                && token[token.len() - 3..].eq_ignore_ascii_case(".rs")
                && !token.contains('*')
            {
                excluded.push(token.to_owned());
            }
            rest = &after[close + 1..];
        }
    }
    excluded
}

/// Every section 10 row has exactly three cells: the subject, the tested
/// count, the verdict. Separators are the pipes outside backticks and
/// outside parentheses (a row may name the `|` operator in prose).
#[test]
fn section_10_rows_have_three_cells() {
    let text = fs::read_to_string(Path::new(ROOT).join(AUDIT)).expect("the audit file exists");
    let section = text
        .split(SECTION)
        .nth(1)
        .unwrap_or_else(|| panic!("{AUDIT} lost its section 10 heading"));
    for line in section.lines() {
        if !line.starts_with('|') {
            continue;
        }
        let mut separators = 0;
        let mut in_ticks = false;
        let mut depth = 0usize;
        for ch in line.chars() {
            match ch {
                '`' => in_ticks = !in_ticks,
                '(' if !in_ticks => depth += 1,
                ')' if !in_ticks => depth = depth.saturating_sub(1),
                '|' if !in_ticks && depth == 0 => separators += 1,
                _ => {}
            }
        }
        assert_eq!(
            separators, 4,
            "{AUDIT} section 10 row must have 3 cells (4 separators, leading              and trailing included): {line}"
        );
    }
}

/// No two section 10 rows are identical: a glued-on copy of an existing
/// row must be deleted, not split out.
#[test]
fn section_10_has_no_duplicate_rows() {
    let text = fs::read_to_string(Path::new(ROOT).join(AUDIT)).expect("the audit file exists");
    let section = text
        .split(SECTION)
        .nth(1)
        .unwrap_or_else(|| panic!("{AUDIT} lost its section 10 heading"));
    let mut seen = std::collections::HashSet::new();
    for line in section.lines() {
        if !line.starts_with('|') {
            continue;
        }
        assert!(
            seen.insert(line),
            "{AUDIT} section 10 has two identical rows: {line}"
        );
    }
}

#[test]
fn section_10_names_every_source_file() {
    let text = fs::read_to_string(Path::new(ROOT).join(AUDIT)).expect("the audit file exists");
    let section = text
        .split(SECTION)
        .nth(1)
        .unwrap_or_else(|| panic!("{AUDIT} lost its section 10 heading"));
    let paths = row_paths(section);
    let all = source_files();
    let excluded = excluded_from_audit(&text);
    for path in &excluded {
        let relative = path.strip_prefix("crates/").unwrap_or(path);
        assert!(
            all.iter().any(|f| f == relative),
            "{AUDIT}'s exclusion block names {path}, which is not a source file on main - drop the entry"
        );
    }
    let unaccounted: Vec<&String> = all
        .iter()
        .filter(|file| {
            !excluded
                .iter()
                .any(|path| path.strip_prefix("crates/") == Some(file.as_str()))
                && !names_file(&paths, file, &all)
        })
        .collect();
    assert!(
        unaccounted.is_empty(),
        "{AUDIT} section 10 must name every source file with a row or an          exclusion with its reason; these have neither: {unaccounted:?}"
    );
}
