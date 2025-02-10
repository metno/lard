use tokio::signal;

/// Returns a Future that completes once a signal to shutdown the service is caught.
pub async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c() // aka. SIGINT on Unix
            .await
            .expect("failed to install Ctrl+C (SIGINT) handler");
    };

    #[cfg(unix)]
    let sighup = async {
        signal::unix::signal(signal::unix::SignalKind::hangup())
            .expect("failed to install signal handler for SIGHUP")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let sighup = std::future::pending::<()>();

    #[cfg(unix)]
    let sigquit = async {
        signal::unix::signal(signal::unix::SignalKind::quit())
            .expect("failed to install signal handler for SIGQUIT")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let sigquit = std::future::pending::<()>();

    #[cfg(unix)]
    let sigpipe = async {
        signal::unix::signal(signal::unix::SignalKind::hangup())
            .expect("failed to install signal handler for SIGPIPE")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let sigpipe = std::future::pending::<()>();

    #[cfg(unix)]
    let sigalrm = async {
        signal::unix::signal(signal::unix::SignalKind::alarm())
            .expect("failed to install signal handler for SIGALRM")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let sigalrm = std::future::pending::<()>();

    #[cfg(unix)]
    let sigterm = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler for SIGTERM")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let sigterm = std::future::pending::<()>();

    #[cfg(unix)]
    let sigchld = async {
        signal::unix::signal(signal::unix::SignalKind::child())
            .expect("failed to install signal handler for SIGCHLD")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let sigchld = std::future::pending::<()>();

    // TODO: add more signals that should result in a shutdown?

    tokio::select! {
        _ = ctrl_c => {},
        _ = sighup => {},
        _ = sigquit => {},
        _ = sigpipe => {},
        _ = sigalrm => {},
        _ = sigterm => {},
        _ = sigchld => {},
    }
}
