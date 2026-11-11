//! The mutation-testing table in section 10 of `docs/production-audit.md`
//! must stay one Markdown table: a blank line between its rows ends the
//! table, and GitHub then renders every later row as plain text with
//! pipes (#1130). The test walks the section's table from its header to
//! its last row and refuses any blank line in between, naming the line.

use std::fs;
use std::path::Path;

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
