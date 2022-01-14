use std::sync::{Arc, Barrier};

use common_base::tokio;
use common_exception::ErrorCode;
use common_exception::Result;
use databend_query::pipelines::new::processors::port::{InputPort, OutputPort};
use databend_query::pipelines::new::processors::{connect};

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_input_and_output_port() -> Result<()> {
    fn input_port(input: InputPort, barrier: Arc<Barrier>) -> impl Fn() + Send {
        move || {
            barrier.wait();
            for index in 0..100 {
                while !input.has_data() {}
                let data = input.pull_data().unwrap();
                assert_eq!(data.unwrap_err().message(), format!("{}", index));
            }
        }
    }

    fn output_port(output: OutputPort, barrier: Arc<Barrier>) -> impl Fn() + Send {
        move || {
            barrier.wait();
            for index in 0..100 {
                while !output.can_push() {}
                output.push_data(Err(ErrorCode::Ok(format!("{}", index))));
            }
        }
    }

    unsafe {
        let input = InputPort::create();
        let output = OutputPort::create();
        let barrier = Arc::new(Barrier::new(2));

        connect(&input, &output);
        let thread_1 = std::thread::spawn(input_port(input, barrier.clone()));
        let thread_2 = std::thread::spawn(output_port(output, barrier.clone()));

        thread_1.join().unwrap();
        thread_2.join().unwrap();
        Ok(())
    }
}

