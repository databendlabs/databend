use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use common_base::base::ThreadPool;
use common_exception::Result;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_thread_pool() -> Result<()> {
    let mut pool = ThreadPool::create(4)?;

    let instant = Instant::now();
    let executed_counter = Arc::new(AtomicUsize::new(0));
    for _index in 0..8 {
        let executed_counter = executed_counter.clone();
        pool.execute(move || {
            std::thread::sleep(Duration::from_secs(3));
            executed_counter.fetch_add(1, Ordering::Release);
        });
    }

    pool.wait()?;

    assert_eq!(executed_counter.load(Ordering::Relaxed), 8);
    assert!(instant.elapsed() < Duration::from_secs(9));
    Ok(())
}