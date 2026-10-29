RUSTSEC-2026-0414: `pulldown-cmark` 0.13.4 allows exponential event
growth with `ENABLE_WIKILINKS`; fixed in 0.13.5, and all four
lockfiles moved to it. Build-time only: the crate enters as a build
dependency of the protobuf codegen, its input is the checked-in
`.proto` comments, and nothing ships the parser. The deps job's
advisory gate now passes at the root and in every fuzz workspace.
