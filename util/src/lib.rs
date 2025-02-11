use tokio::signal;

/// Returns a Future that completes once a signal to shutdown the service is caught.
pub async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c() // aka. SIGINT on Unix
            .await
            .expect("failed to install Ctrl+C (SIGINT) handler");
    };

    #[cfg(unix)]
    let sigterm = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler for SIGTERM")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let sigterm = std::future::pending::<()>();

    // TODO: add more signals that should result in a shutdown?

    tokio::select! {
        _ = ctrl_c => {},
        _ = sigterm => {},
    }
}
