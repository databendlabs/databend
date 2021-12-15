use std::sync::Arc;
use common_base::tokio;
use common_exception::{ErrorCode, Result};
use common_infallible::Mutex;
use databend_query::pipelines::new::processors::port::{ReactiveInputPort, ReactiveOutputPort, SharedStatus};
use databend_query::pipelines::new::processors::PortReactor;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_input_and_output_port() -> Result<()> {
    struct TestPortReactor;

    impl PortReactor<usize> for TestPortReactor {
        fn on_push(&self, push_to: usize) {
            assert_eq!(push_to, 2);
        }

        fn on_pull(&self, pull_from: usize) {
            assert_eq!(pull_from, 1);
        }
    }

    let reactor = Arc::new(TestPortReactor {});
    let shared_status = SharedStatus::create();
    let input = ReactiveInputPort::<usize, TestPortReactor>::create(shared_status.clone(), reactor.clone(), 1);
    let output = ReactiveOutputPort::<usize, TestPortReactor>::create(shared_status, reactor, 2);
    assert!(input.pull_data().is_none());
    output.push_data(Err(ErrorCode::LogicalError("Logical error")));
    let data = input.pull_data();
    assert!(data.is_some());
    assert_eq!(data.unwrap().unwrap_err().message(), "Logical error");

    Ok(())
}