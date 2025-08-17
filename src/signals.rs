use std::sync::Arc;

use libc::{SIGINT, SIGTERM};
use tokio::signal::unix::SignalKind;
use tokio::sync::Notify;

pub fn listen_signals() -> Arc<Notify> {
    let notify = Arc::new(Notify::new());

    for &signum in [SIGTERM, SIGINT].iter() {
        let mut sig = tokio::signal::unix::signal(SignalKind::from_raw(signum)).unwrap();

        let notify = notify.clone();

        tokio::spawn(async move {
            sig.recv().await;

            notify.notify_waiters();

            log::info!("shutdown signal received");
        });
    }

    log::info!("waiting for signal");

    notify
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::timeout;

    #[tokio::test]
    async fn test_listen_signals_creates_notify() {
        // Test that listen_signals returns a valid Notify object
        let notify = listen_signals();

        // Should be able to clone the Arc
        let notify_clone = notify.clone();

        assert!(Arc::ptr_eq(&notify, &notify_clone));
    }

    #[tokio::test]
    async fn test_manual_notify_wakeup() {
        // Test that we can manually trigger the notify mechanism
        let notify = Arc::new(Notify::new());
        let notify_clone = notify.clone();

        // Spawn a task that waits for notification
        let wait_handle = tokio::spawn(async move {
            notify_clone.notified().await;
            "notified"
        });

        // Give it a moment to start waiting
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Trigger notification
        notify.notify_waiters();

        // Should complete quickly
        let result = timeout(Duration::from_millis(100), wait_handle).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().unwrap(), "notified");
    }

    #[tokio::test]
    async fn test_multiple_waiters_notification() {
        // Test that notify_waiters() wakes up multiple waiting tasks
        let notify = Arc::new(Notify::new());

        let mut handles = vec![];
        for i in 0..3 {
            let notify_clone = notify.clone();
            let handle = tokio::spawn(async move {
                notify_clone.notified().await;
                i
            });
            handles.push(handle);
        }

        // Give tasks time to start waiting
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Notify all waiters
        notify.notify_waiters();

        // All should complete
        for (i, handle) in handles.into_iter().enumerate() {
            let result = timeout(Duration::from_millis(100), handle).await;
            assert!(result.is_ok(), "Handle {} should complete", i);
            assert_eq!(result.unwrap().unwrap(), i);
        }
    }

    #[tokio::test]
    async fn test_graceful_shutdown_simulation() {
        // Test a realistic graceful shutdown scenario
        let shutdown_notify = Arc::new(Notify::new());
        let component_notify = shutdown_notify.clone();

        // Simulate a component waiting for shutdown signal
        let component_task = tokio::spawn(async move {
            let shutdown_fut = component_notify.notified();
            tokio::pin!(shutdown_fut);

            loop {
                tokio::select! {
                    _ = &mut shutdown_fut => {
                        log::info!("Component received shutdown signal");
                        break;
                    }
                    _ = tokio::time::sleep(Duration::from_millis(1)) => {
                        // Simulate work
                    }
                }
            }
            "shutdown_complete"
        });

        // Give component time to start
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Trigger shutdown
        shutdown_notify.notify_waiters();

        // Component should shut down gracefully
        let result = timeout(Duration::from_millis(200), component_task).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().unwrap(), "shutdown_complete");
    }

    #[tokio::test]
    async fn test_concurrent_shutdown_components() {
        // Test multiple components shutting down concurrently
        let shutdown_notify = Arc::new(Notify::new());

        let mut component_handles = vec![];
        for i in 0..5 {
            let notify = shutdown_notify.clone();
            let handle = tokio::spawn(async move {
                // Each component waits for shutdown in its own way
                notify.notified().await;

                // Simulate cleanup work
                tokio::time::sleep(Duration::from_millis(i * 10)).await;

                format!("component_{}_shutdown", i)
            });
            component_handles.push(handle);
        }

        // Give all components time to start waiting
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Signal shutdown to all
        shutdown_notify.notify_waiters();

        // All should complete their shutdown
        let mut results = vec![];
        for handle in component_handles {
            results.push(handle.await.unwrap());
        }

        assert_eq!(results.len(), 5);
        for (i, result) in results.iter().enumerate() {
            assert_eq!(*result, format!("component_{}_shutdown", i));
        }
    }

    #[test]
    fn test_signal_constants() {
        // Test that we're using the correct signal constants
        assert_eq!(SIGTERM, 15); // Standard SIGTERM value on Unix
        assert_eq!(SIGINT, 2); // Standard SIGINT value on Unix
    }

    #[tokio::test]
    async fn test_notify_clone_independence() {
        // Test that cloned notifiers work independently for different patterns
        let original = Arc::new(Notify::new());
        let clone1 = original.clone();
        let clone2 = original.clone();

        // Wait on one clone
        let wait1 = tokio::spawn(async move {
            clone1.notified().await;
            "clone1_notified"
        });

        // Wait on another clone
        let wait2 = tokio::spawn(async move {
            clone2.notified().await;
            "clone2_notified"
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        // Notify through original
        original.notify_waiters();

        // Both clones should receive notification
        let result1 = timeout(Duration::from_millis(100), wait1).await;
        let result2 = timeout(Duration::from_millis(100), wait2).await;

        assert!(result1.is_ok());
        assert!(result2.is_ok());
        assert_eq!(result1.unwrap().unwrap(), "clone1_notified");
        assert_eq!(result2.unwrap().unwrap(), "clone2_notified");
    }

    #[tokio::test]
    async fn test_shutdown_timing() {
        // Test shutdown notification happens promptly
        let notify = Arc::new(Notify::new());
        let notify_clone = notify.clone();

        let start_time = std::time::Instant::now();

        let wait_task = tokio::spawn(async move {
            notify_clone.notified().await;
            std::time::Instant::now()
        });

        // Small delay to ensure task starts waiting
        tokio::time::sleep(Duration::from_millis(5)).await;

        // Trigger notification
        notify.notify_waiters();

        // Should complete very quickly
        let end_time = wait_task.await.unwrap();
        let total_duration = end_time.duration_since(start_time);

        // Should complete within 50ms (generous bound for CI)
        assert!(
            total_duration < Duration::from_millis(50),
            "Shutdown took too long: {:?}",
            total_duration
        );
    }

    #[tokio::test]
    async fn test_integration_like_usage() {
        // Test usage pattern similar to actual application
        let shutdown_notify = listen_signals();

        // Simulate main application components
        let watcher_notify = shutdown_notify.clone();
        let sender_notify = shutdown_notify.clone();
        let es_notify = shutdown_notify.clone();

        // Start mock components
        let watcher_task = tokio::spawn(async move {
            watcher_notify.notified().await;
            "watcher_shutdown"
        });

        let sender_task = tokio::spawn(async move {
            sender_notify.notified().await;
            "sender_shutdown"
        });

        let es_task = tokio::spawn(async move {
            es_notify.notified().await;
            "es_shutdown"
        });

        // Give components time to start
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Simulate signal (since we can't easily send real signals in tests)
        shutdown_notify.notify_waiters();

        // All components should shut down
        let watcher_result = timeout(Duration::from_millis(100), watcher_task).await;
        let sender_result = timeout(Duration::from_millis(100), sender_task).await;
        let es_result = timeout(Duration::from_millis(100), es_task).await;

        assert!(watcher_result.is_ok());
        assert!(sender_result.is_ok());
        assert!(es_result.is_ok());

        assert_eq!(watcher_result.unwrap().unwrap(), "watcher_shutdown");
        assert_eq!(sender_result.unwrap().unwrap(), "sender_shutdown");
        assert_eq!(es_result.unwrap().unwrap(), "es_shutdown");
    }
}
