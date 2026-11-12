//! `shutdown_signal` resolves on SIGTERM: the server's own signal driver
//! catches it, so the process survives to end cleanly. The test lives in
//! its own binary: it registers the SIGTERM handler, and the handler
//! swallows SIGTERM for the rest of the process, which would silence any
//! later signal test in the same binary.

#[tokio::test]
async fn shutdown_signal_resolves_on_sigterm() {
    let mut listening = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("the SIGTERM driver registers");
    let mut waiting = tokio::spawn(scion_proto::runtime::metrics::shutdown_signal());
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    // The future must still be waiting: a stub that resolves without the
    // signal fails here, before the kill can excuse it.
    tokio::time::timeout(std::time::Duration::from_millis(300), &mut waiting)
        .await
        .expect_err("shutdown_signal holds until the signal arrives");
    std::process::Command::new("kill")
        .args(["-TERM", &std::process::id().to_string()])
        .status()
        .expect("kill runs");
    tokio::time::timeout(std::time::Duration::from_secs(10), waiting)
        .await
        .expect("shutdown_signal resolves on SIGTERM")
        .unwrap();
    let _ = listening.recv().await;
}
