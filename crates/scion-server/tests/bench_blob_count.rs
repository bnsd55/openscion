//! The blob-count measurement (issue #823): fills a store with small
//! blobs through the store's own put path, then times the server's
//! open, `fsck` and `gc --dry-run` over the result, and prints the
//! bytes on disk. Runs in release mode only, only when asked: a run of
//! millions of blobs takes tens of minutes and its numbers belong on a
//! fixed runner, not a laptop. The count comes from
//! `SCION_BENCH_BLOB_COUNT`; the store lives under `SCION_BENCH_DIR`.
//!
//! Every fact prints as one `BENCH <key> <value>` line, so the driving
//! script can lift them into a table without parsing prose.

use std::time::Duration;
use std::time::Instant;

use scion_server::{Backend, fsck, gc};

/// One small blob's content: 64 bytes, unique per blob, so the store
/// keeps one file for each. A monorepo's smallest files are this size.
fn blob_content(i: u64) -> [u8; 64] {
    let mut raw = [0u8; 64];
    raw[..8].copy_from_slice(&i.to_be_bytes());
    // The rest of the bytes repeat the counter's low byte: the
    // counter's own bytes already make the content unique, and the
    // padding makes the blob small but not degenerate.
    raw[8..].fill(u8::try_from(i % 256).unwrap_or(0));
    raw
}

/// The count of blobs the run fills, from the environment.
fn count() -> u64 {
    std::env::var("SCION_BENCH_BLOB_COUNT")
        .expect("SCION_BENCH_BLOB_COUNT names the blob count")
        .parse()
        .expect("SCION_BENCH_BLOB_COUNT is a number")
}

/// The data directory the run fills, from the environment.
fn bench_dir() -> std::path::PathBuf {
    std::path::PathBuf::from(
        std::env::var("SCION_BENCH_DIR").expect("SCION_BENCH_DIR names the store's directory"),
    )
}

fn main_line(key: &str, value: impl std::fmt::Display) {
    println!("BENCH {key} {value}");
}

/// Fills `count` small blobs through the store's own put path: a batch
/// of the blob folder, one put per blob, one sync every 4,096 writes.
/// The syncs make the store durable the way a client's puts are, and
/// cost the run nothing it would not pay in production. The put path
/// dedupes by content, so every blob's content is unique.
fn fill(backend: &Backend, count: u64) {
    // One sync per 4,096 blobs: the durability of a client's puts, at a
    // fraction of the sync calls.
    const SYNC_EVERY: u64 = 4096;
    let started = Instant::now();
    let mut i = 0u64;
    while i < count {
        let top = (i + SYNC_EVERY).min(count);
        let mut batch = backend.blobs().batch();
        for n in i..top {
            batch.put(&blob_content(n)).expect("the blob is stored");
        }
        batch.sync().expect("the batch is durable");
        i = top;
    }
    main_line(
        "fill_seconds",
        format!("{:.3}", started.elapsed().as_secs_f64()),
    );
}

/// The bytes this process asked of the storage device since the last
/// call, from `/proc/self/io`'s `read_bytes`. A page-cache hit never
/// reaches this counter, so against the blob count it says whether the
/// walk reads storage per file.
#[cfg(target_os = "linux")]
fn storage_read_delta() -> u64 {
    thread_local! {
        static LAST: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
    }
    let now: Option<u64> = std::fs::read_to_string("/proc/self/io")
        .ok()
        .and_then(|io| {
            io.lines()
                .find(|line| line.starts_with("read_bytes:"))?
                .split_whitespace()
                .nth(1)?
                .parse()
                .ok()
        });
    LAST.with(|slot| slot.replace(now).map_or(0, |last| now.unwrap_or(0) - last))
}

/// The counter is Linux's; elsewhere the walk names no storage reads.
#[cfg(not(target_os = "linux"))]
fn storage_read_delta() -> u64 {
    0
}

#[test]
#[ignore = "a measurement, not a proof: run it in release mode with SCION_BENCH_BLOB_COUNT set"]
fn blob_count_costs() {
    let count = count();
    let dir = bench_dir();
    scion_proto::runtime::auth::create_private_dir_all(&dir).unwrap();

    // The open the server does: the store's open, whose Postgres half
    // the test backend shares the harness's URL for.
    let started = Instant::now();
    let backend = Backend::open_test(&dir).expect("the store opens");
    let open = started.elapsed();
    main_line("open_seconds", format!("{:.3}", open.as_secs_f64()));
    main_line("blob_count", count);

    fill(&backend, count);
    main_line("blob_bytes", count * 64);

    drop(backend);

    // The open after the fill: the open a restart pays, with the blob
    // folders in place.
    let started = Instant::now();
    let backend = Backend::open_test(&dir).expect("the store opens again");
    let open = started.elapsed();
    main_line("reopen_seconds", format!("{:.3}", open.as_secs_f64()));

    // fsck walks every blob file, then the metadata tables.
    let started = Instant::now();
    let report = fsck::run(&backend, fsck::BATCH, &mut |_| {}).expect("fsck runs");
    main_line(
        "fsck_seconds",
        format!("{:.3}", started.elapsed().as_secs_f64()),
    );
    main_line("fsck_problems", report.problems.len());
    // The bytes the walk asked of the storage device, not the page
    // cache: /proc/self/io's read_bytes counts them on Linux, and the
    // runner is Linux. Against the count and the blob bytes this says
    // whether a large walk pays a storage read per file.
    let fsck_reads = storage_read_delta();
    main_line("fsck_storage_read_bytes", fsck_reads);

    // gc --dry-run walks the blob files and answers what it would
    // reclaim; it writes nothing.
    let started = Instant::now();
    let report = gc::run(
        &backend,
        Duration::from_secs(gc::DEFAULT_GRACE_SECS),
        true,
        &mut |_| {},
    )
    .expect("gc --dry-run runs");
    main_line(
        "gc_dry_run_seconds",
        format!("{:.3}", started.elapsed().as_secs_f64()),
    );
    main_line("gc_storage_read_bytes", storage_read_delta());
    main_line("gc_dry_run_files_young", report.files_young);
    main_line("gc_dry_run_files_removed", report.files_removed);
}
