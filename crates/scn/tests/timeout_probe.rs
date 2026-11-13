//! TEMPORARY probe for #1197, removed once its proof is read: a test
//! that sleeps past the ci profile's 20-minute kill, so the job's log
//! names it instead of the job dying at its cap with no test named.

#[test]
fn the_probe_sleeps_past_the_kill() {
    std::thread::sleep(std::time::Duration::from_secs(21 * 60));
}
