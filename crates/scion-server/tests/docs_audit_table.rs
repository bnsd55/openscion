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

/// The files the audit names as excluded, with the reason. A row that
/// names one of these replaces its entry, and the entry is dropped.
const EXCLUSIONS: &[(&str, &str)] = &[
    // (path relative to crates/, the audit's reason)
    (
        "scion-server/src/test_support/backend.rs",
        "test-only helpers",
    ),
    (
        "scion-server/src/test_support/damage.rs",
        "test-only helpers",
    ),
    ("scion-server/src/test_support/db.rs", "test-only helpers"),
    ("scion-server/src/test_support/docs.rs", "test-only helpers"),
    ("scion-server/src/test_support/pg.rs", "test-only helpers"),
    (
        "scion-server/src/test_support/store.rs",
        "test-only helpers",
    ),
    ("scion-server/src/pause.rs", "test-only helpers"),
    ("scion-server/src/seed.rs", "test-only helpers"),
    (
        "scion-server/src/lib.rs",
        "test-only helpers behind a feature",
    ),
    ("scn/src/lib.rs", "test-only helpers"),
    ("scion-proto/src/runtime/mod.rs", "test-only helpers"),
    ("scion-server/src/auth.rs", "re-export only"),
    ("scn/src/cmd/mod.rs", "re-export only"),
    ("scion-proto/src/lib.rs", "no branch"),
    ("scion-server/src/pg.rs", "tracked by #1060"),
    ("scion-server/src/tickets.rs", "tracked by #1192"),
    ("scion-server/src/names.rs", "tracked by #1192"),
    ("scion-tier/src/bin/scion-cache.rs", "tracked by #1191"),
    ("scion-tier/src/bin/scion-daemon.rs", "tracked by #1191"),
    ("scn/src/cmd/land.rs", "tracked by #1199"),
    ("scn/src/cmd/blame.rs", "tracked by #1199"),
    ("scn/src/cmd/query.rs", "tracked by #1199"),
    ("scn/src/cmd/read.rs", "tracked by #1199"),
    ("scn/src/out.rs", "tracked by #1199"),
    ("scion-proto/src/runtime/auth/ffi.rs", "tracked by #1200"),
    (
        "scion-proto/src/runtime/auth/removed.rs",
        "tracked by #1200",
    ),
    ("scion-proto/src/runtime/deadline.rs", "tracked by #1200"),
    ("scion-proto/src/runtime/host.rs", "tracked by #1200"),
    ("scion-proto/src/runtime/reply.rs", "tracked by #1200"),
    ("scion-proto/src/runtime/metrics.rs", "tracked by #1200"),
    ("scion-server/src/metrics.rs", "tracked by #1200"),
];

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
    let Some(rel) = file.split_once("/src/").map(|(_, rest)| rest) else {
        return false;
    };
    // A row's token drops the `src/` segment: `runtime/deadline.rs`
    // names scion-server/src/deadline.rs when it ends with it, and a
    // bare basename matches only when unique (checked below).
    let without_src = rel.to_owned();
    if paths
        .iter()
        .any(|p| p.ends_with(&without_src) && p.contains('/'))
    {
        return true;
    }
    let basename = rel.rsplit('/').next().unwrap();
    let unique = all
        .iter()
        .filter(|f| f.rsplit('/').next() == Some(basename))
        .count()
        == 1;
    unique && paths.iter().any(|p| p == basename)
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
    let unaccounted: Vec<&String> = all
        .iter()
        .filter(|file| {
            !EXCLUSIONS.iter().any(|(path, _)| path == *file) && !names_file(&paths, file, &all)
        })
        .collect();
    assert!(
        unaccounted.is_empty(),
        "{AUDIT} section 10 must name every source file with a row or an          exclusion with its reason; these have neither: {unaccounted:?}"
    );
}
