use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use common_base::base::ThreadPool;
use common_exception::Result;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_thread_pool() -> Result<()> {
    let pool = ThreadPool::create(4)?;

    let instant = Instant::now();
    let mut join_handles = Vec::with_capacity(8);
    let executed_counter = Arc::new(AtomicUsize::new(0));
    for _index in 0..8 {
        let executed_counter = executed_counter.clone();
        join_handles.push(pool.execute(move || {
            std::thread::sleep(Duration::from_secs(3));
            executed_counter.fetch_add(1, Ordering::Release);
        }));
    }

    for join_handle in join_handles {
        join_handle.join();
    }

    assert_eq!(executed_counter.load(Ordering::Relaxed), 8);
    assert!(instant.elapsed() < Duration::from_secs(9));
    Ok(())
}
