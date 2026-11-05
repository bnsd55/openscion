//! The blob-count measurement (issue #823): fills a store with small
//! blobs through the store's own put path, then times the server's
//! open, `fsck` and `gc --dry-run` over the result, and prints the
//! bytes on disk. Runs in release mode only, only when asked: a run of
//! millions of blobs takes tens of minutes and its numbers belong on
//! the fixed CI runner. The count comes from
//! `SCION_BENCH_BLOB_COUNT`; the store lives under `SCION_BENCH_DIR`.
//!
//! Every fact prints as one `BENCH <key> <value>` line, so the driving
//! script can lift them into a table without parsing prose.

use std::time::Duration;
use std::time::Instant;

use scion_server::test_support::meta::{Meta as _, MetaTx as _};
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

/// This process's own CPU time (utime) and its kernel time (stime), in
/// seconds, from `/proc/self/stat`. The split of the walk's wall time
/// says whether a growing cost is the process's work or the kernel's.
#[cfg(target_os = "linux")]
fn cpu_seconds() -> (f64, f64) {
    // The clock ticks a stat's times are counted in.
    const TICKS: f64 = 100.0;
    let stat = std::fs::read_to_string("/proc/self/stat").unwrap();
    let body = stat.rsplit_once(')').unwrap().1;
    let fields: Vec<&str> = body.split_whitespace().collect();
    // After the comm field, the tokens start at `state` (field 3), so
    // utime (field 14) is token 11 and stime (field 15) is token 12.
    let utime: f64 = fields[11].parse().unwrap();
    let stime: f64 = fields[12].parse().unwrap();
    (utime / TICKS, stime / TICKS)
}

#[cfg(not(target_os = "linux"))]
fn cpu_seconds() -> (f64, f64) {
    (0.0, 0.0)
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
    let dir = bench_dir();
    // An fsck-only pass reuses a store the script already built: the
    // walk's shape questions are answered on the same bytes.
    let fsck_only = std::env::var("SCION_BENCH_FSCK_ONLY").is_ok_and(|v| v == "1");
    let count = if fsck_only { 0 } else { count() };
    scion_proto::runtime::auth::create_private_dir_all(&dir).unwrap();

    // The open the server does: the store's open, whose Postgres half
    // the test backend shares the harness's URL for.
    let started = Instant::now();
    let backend = Backend::open_test(&dir).expect("the store opens");
    let open = started.elapsed();
    main_line("open_seconds", format!("{:.3}", open.as_secs_f64()));
    main_line("blob_count", count);

    if count > 0 {
        fill(&backend, count);
    }
    main_line("blob_bytes", count * 64);

    // The metadata's blob rows: the count gauge's own subject. One row
    // per stored blob, added in the store's own batch form (one
    // statement per 4,096), so the count query below reads a table of
    // the same size as the store's files.
    if count > 0 {
        let started = Instant::now();
        backend
            .write(|c| {
                let repo = c.create_repo("bench")?;
                let mut i = 0u64;
                while i < count {
                    let top = (i + 4096).min(count);
                    let ids: Vec<scion_core::BlobId> = (i..top)
                        .map(|n| scion_core::BlobId::of(&blob_content(n)))
                        .collect();
                    c.add_repo_blobs(repo, &ids)?;
                    i = top;
                }
                Ok(())
            })
            .expect("the blob rows are written");
        main_line(
            "blob_rows_seconds",
            format!("{:.3}", started.elapsed().as_secs_f64()),
        );
    }

    // The count query the 60 s poll runs: five runs, each printed; the
    // driver reports the middle one. One bounded statement over the
    // blob rows, never a directory walk.
    for run in 0..5 {
        let started = Instant::now();
        let counted = backend.read(|c| c.blob_count()).expect("the count runs");
        main_line(
            "blob_count_query_ms",
            format!("{run} {:.3}", started.elapsed().as_secs_f64() * 1000.0),
        );
        assert_eq!(
            counted, count,
            "the count query counts the rows the run wrote"
        );
    }

    drop(backend);

    // The open after the fill: the open a restart pays, with the blob
    // folders in place.
    let started = Instant::now();
    let backend = Backend::open_test(&dir).expect("the store opens again");
    let open = started.elapsed();
    main_line("reopen_seconds", format!("{:.3}", open.as_secs_f64()));

    // fsck walks every blob file, then the metadata tables.
    let (u0, s0) = cpu_seconds();
    let started = Instant::now();
    let report = fsck::run(&backend, fsck::BATCH, &mut |_| {}).expect("fsck runs");
    main_line(
        "fsck_seconds",
        format!("{:.3}", started.elapsed().as_secs_f64()),
    );
    let (u1, s1) = cpu_seconds();
    main_line("fsck_utime_s", format!("{:.3}", u1 - u0));
    main_line("fsck_stime_s", format!("{:.3}", s1 - s0));
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

    // The same walk again, in the same process: a second run that
    // matches the first says the cost follows the store's size, not
    // this process's history; a second run that differs says the
    // process carried state in.
    let (u0, s0) = cpu_seconds();
    let started = Instant::now();
    let report = fsck::run(&backend, fsck::BATCH, &mut |_| {}).expect("the second fsck runs");
    main_line(
        "fsck2_seconds",
        format!("{:.3}", started.elapsed().as_secs_f64()),
    );
    let (u1, s1) = cpu_seconds();
    main_line("fsck2_utime_s", format!("{:.3}", u1 - u0));
    main_line("fsck2_stime_s", format!("{:.3}", s1 - s0));
    main_line("fsck2_problems", report.problems.len());
}
