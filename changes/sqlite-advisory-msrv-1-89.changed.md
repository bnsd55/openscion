The minimum supported Rust rises from 1.88 to 1.89. The SQLite advisory
RUSTSEC-2026-0459 is fixed only in libsqlite3-sys 0.39, and the smallest
rusqlite that accepts it is 0.41, which declares Rust 1.89. The bundled
SQLite stays: the tier's index still ships its own engine. The MSRV job,
the builder image and the docs name 1.89 together with this change.
