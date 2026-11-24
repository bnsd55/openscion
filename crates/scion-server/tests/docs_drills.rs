//! Every command a drill's shell blocks copy from docs/operations.md
//! appears there, so a drill whose commands drift from the runbooks
//! fails here (#1407). The drills that claim the copy are listed in
//! `DRILLS`; the runbook side is the whole of operations.md's own `sh`
//! blocks.
//!
//! The comparison is light, and the same on both sides: shell
//! continuations join into one command, an unquoted trailing comment is
//! prose and goes, whitespace runs collapse, and the `./` prefix of a
//! binary goes. A drill line that is the drill's own - a check the
//! drill adds, not a runbook command - is marked in the drill with a
//! trailing `# drill-only` comment, and the test skips it.

use std::collections::HashSet;
use std::path::Path;

/// The drills that say their commands are copied from operations.md.
/// A drill that makes the same claim adds its name here.
const DRILLS: [&str; 2] = ["operator-drill", "second-operator"];

/// The comment a drill ends its own lines with: a check the drill adds,
/// not a command the runbook carries.
const DRILL_ONLY: &str = "# drill-only";

/// Strips an unquoted trailing comment: a `#` outside quotes with a
/// space before it starts prose, so `grep '^scion_x'` keeps its pattern
/// and `cmd # why` loses it.
fn strip_comment(line: &str) -> &str {
    let mut quote = None;
    for (at, ch) in line.char_indices() {
        if quote == Some(ch) {
            quote = None;
        } else if quote.is_none() {
            if ch == '"' || ch == '\'' {
                quote = Some(ch);
            } else if ch == '#' && line[..at].ends_with(' ') {
                return &line[..at];
            }
        }
    }
    line
}

/// The command lines of a document's `sh` blocks, continuations joined:
/// one string per command, with the 1-based line the command starts on.
fn sh_commands(doc: &str) -> Vec<(usize, String)> {
    let mut commands = Vec::new();
    let mut in_block = false;
    let mut start = 0;
    let mut current = String::new();
    for (at, line) in doc.lines().enumerate() {
        let trimmed = line.trim();
        if !in_block {
            if trimmed == "```sh" {
                in_block = true;
                start = at + 1;
                current.clear();
            }
            continue;
        }
        if trimmed == "```" {
            in_block = false;
            if !current.trim().is_empty() {
                commands.push((start, current.trim().to_string()));
            }
            continue;
        }
        // A comment that ends a line marks the whole command it is on:
        // either prose (operations.md documents why) or the drill's own
        // `# drill-only` mark.
        let body = strip_comment(line.trim());
        let continuation = body.ends_with('\\');
        let body = body.trim_end_matches('\\').trim();
        if !current.is_empty() {
            current.push(' ');
        }
        current.push_str(body);
        if !continuation && !current.trim().is_empty() {
            commands.push((start, current.trim().to_string()));
            current.clear();
        }
    }
    commands
}

/// The light normalization, the same on both sides.
fn normalize(command: &str) -> String {
    let no_prefix = command.strip_prefix("./").unwrap_or(command);
    no_prefix.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn doc(name: &str) -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(format!("../../docs/{name}.md"));
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

#[test]
fn every_command_the_drills_copy_is_in_the_operations_guide() {
    let runbook: HashSet<String> = sh_commands(&doc("operations"))
        .into_iter()
        .map(|(_, c)| normalize(&c))
        .collect();
    assert!(
        runbook.len() >= 20,
        "operations.md's sh blocks shrank to {} commands; the runbook side of this test reads nothing",
        runbook.len()
    );

    let mut misses = Vec::new();
    for drill in DRILLS {
        let commands = sh_commands(&doc(&format!("drills/{drill}")));
        assert!(
            commands.len() >= 8,
            "{drill}.md's sh blocks shrank to {} commands; the drill side of this test reads nothing",
            commands.len()
        );
        for (line, command) in commands {
            if command.ends_with(DRILL_ONLY) {
                continue;
            }
            let normalized = normalize(&command);
            if normalized.starts_with('#') || runbook.contains(&normalized) {
                continue;
            }
            misses.push(format!(
                "{drill}.md:{line}: {command} - not in docs/operations.md"
            ));
        }
    }
    assert!(
        misses.is_empty(),
        "the drills' commands drifted from the runbooks:\n{}",
        misses.join("\n")
    );
}
