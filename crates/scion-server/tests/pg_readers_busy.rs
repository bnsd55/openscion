//! The readers' busy gauge: a reader out of the pool reads
//! `scion_pg_readers_busy` at least 1, and a reader back in the pool
//! reads it 0. The gauge is the operator's view of the read pool's
//! occupancy; a gauge that never leaves zero hides a starved pool.

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use scion_server::Backend;
use scion_server::test_support::meta::Meta as _;

#[test]
fn a_reader_out_of_the_pool_reads_busy_and_a_returned_one_reads_zero() {
    let dir = scion_server::test_support::private_tempdir();
    let store = Backend::open_test(dir.path()).unwrap();
    let metrics = Arc::new(scion_server::Metrics::default());
    store.set_metrics(Arc::clone(&metrics));

    // One reader leaves the pool and stays out while the scrape reads.
    let held = Arc::new(AtomicU8::new(0));
    let reader = {
        let held = Arc::clone(&held);
        std::thread::spawn(move || {
            store.read(|_tx| {
                held.store(1, Ordering::SeqCst);
                std::thread::sleep(std::time::Duration::from_millis(1500));
                Ok(())
            })
        })
    };
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while held.load(Ordering::SeqCst) == 0 && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    assert_eq!(held.load(Ordering::SeqCst), 1, "the read took its reader");

    let busy = |text: &str| {
        text.lines()
            .find(|line| line.starts_with("scion_pg_readers_busy "))
            .and_then(|line| line.rsplit(' ').next())
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or_else(|| panic!("no scion_pg_readers_busy line in:\n{text}"))
    };
    let busy_held = busy(&metrics.render());
    assert!(
        busy_held >= 1,
        "the out-of-pool reader reads busy, not {busy_held}: {}",
        metrics.render()
    );

    reader.join().unwrap().expect("the read answered");

    let busy_back = busy(&metrics.render());
    assert_eq!(busy_back, 0, "the returned reader reads idle");
}
