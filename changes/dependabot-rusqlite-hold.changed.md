Dependabot no longer offers rusqlite 0.41 anywhere: the version needs
Rust 1.89, above the 1.88 MSRV the build pins (#715). The ignore holds
the root entry and every fuzz sub-workspace that resolves rusqlite
(scion-server and scion-tier; core and proto carry none, and stay
covered should one appear). Lift the ignores when the MSRV rises
(#709 was closed for the same reason).
