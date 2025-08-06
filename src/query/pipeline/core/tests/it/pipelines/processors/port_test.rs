// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;
use std::sync::Barrier;

use databend_common_base::runtime::Thread;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::local_block_meta_serde;
use databend_common_expression::types::Int32Type;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_pipeline_core::processors::connect;
use databend_common_pipeline_core::processors::BlockLimit;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;

#[derive(Clone, Debug)]
struct TestDataMeta {
    ref_count: Arc<()>,
}

impl TestDataMeta {
    pub fn new() -> TestDataMeta {
        TestDataMeta {
            ref_count: Arc::new(()),
        }
    }

    pub fn ref_count(&self) -> usize {
        Arc::strong_count(&self.ref_count)
    }
}

local_block_meta_serde!(TestDataMeta);

#[typetag::serde(name = "test_data_meta")]
impl BlockMetaInfo for TestDataMeta {
    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_port_drop() -> Result<()> {
    let meta = TestDataMeta::new();

    assert_eq!(meta.ref_count(), 1);
    unsafe {
        let input = InputPort::create();
        let output = OutputPort::create();

        connect(&input, &output, Arc::new(BlockLimit::default()));
        output.push_data(Ok(DataBlock::empty_with_meta(meta.clone_self())));
        assert_eq!(meta.ref_count(), 2);
    }

    assert_eq!(meta.ref_count(), 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_input_and_output_port() -> Result<()> {
    fn input_port(input: Arc<InputPort>, barrier: Arc<Barrier>) -> impl Fn() + Send {
        move || {
            barrier.wait();
            for index in 0..100 {
                input.set_need_data();
                while !input.has_data() {}
                let data = input.pull_data().unwrap();
                assert_eq!(data.unwrap_err().message(), index.to_string());
            }
        }
    }

    fn output_port(output: Arc<OutputPort>, barrier: Arc<Barrier>) -> impl Fn() + Send {
        move || {
            barrier.wait();
            for index in 0..100 {
                while !output.can_push() {}
                output.push_data(Err(ErrorCode::Ok(index.to_string())));
            }
        }
    }

    unsafe {
        let input = InputPort::create();
        let output = OutputPort::create();
        let barrier = Arc::new(Barrier::new(2));

        connect(&input, &output, Arc::new(BlockLimit::default()));
        let thread_1 = Thread::spawn(input_port(input, barrier.clone()));
        let thread_2 = Thread::spawn(output_port(output, barrier));

        thread_1.join().unwrap();
        thread_2.join().unwrap();
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_input_and_output_flags() -> Result<()> {
    unsafe {
        let input = InputPort::create();
        let output = OutputPort::create();

        connect(&input, &output, Arc::new(BlockLimit::default()));

        output.finish();
        assert!(input.is_finished());
        input.set_need_data();
        assert!(input.is_finished());
    }

    // assert_eq!(output.can_push());
    // input.set_need_data();
    // assert_eq!(!output.can_push());
    Ok(())
}

#[test]
fn test_block_limit_splitting() -> Result<()> {
    unsafe {
        let input = InputPort::create();
        let output = OutputPort::create();

        // Create a BlockLimit with small row limit to trigger splitting
        let block_limit = Arc::new(BlockLimit::new(10, 1024 * 1024)); // 10 rows max, 1MB bytes
        connect(&input, &output, block_limit);

        // Create a block with 30 rows
        let mut data = Vec::with_capacity(30);
        for i in 0..30 {
            data.push(i);
        }
        let col = Int32Type::from_data(data);
        let block = DataBlock::new_from_columns(vec![col]);

        // Push the large block
        output.push_data(Ok(block.clone()));

        // First pull should get 10 rows
        input.set_need_data();
        assert!(input.has_data());
        let pulled_block = input.pull_data().unwrap()?;
        assert_eq!(pulled_block.num_rows(), 10);

        // After first pull, should still have data (remaining 20 rows)
        assert!(input.has_data());

        // Second pull should get another 10 rows
        let pulled_block = input.pull_data().unwrap()?;
        assert_eq!(pulled_block.num_rows(), 10);

        // Should still have data (remaining 10 rows)
        assert!(input.has_data());

        // Third pull should get the last 10 rows
        let pulled_block = input.pull_data().unwrap()?;
        assert_eq!(pulled_block.num_rows(), 10);

        // Now should have no data
        assert!(!input.has_data());

        // Trying to pull again should return None
        let result = input.pull_data();
        assert!(result.is_none());
    }

    Ok(())
}

#[test]
fn test_block_limit_no_splitting() -> Result<()> {
    unsafe {
        let input = InputPort::create();
        let output = OutputPort::create();

        // Create a BlockLimit with large limits (no splitting should occur)
        let block_limit = Arc::new(BlockLimit::new(1000, 10 * 1024 * 1024)); // 1000 rows, 10MB
        connect(&input, &output, block_limit);

        // Create a small block with 30 rows
        let mut data = Vec::with_capacity(30);
        for i in 0..30 {
            data.push(i);
        }
        let col = Int32Type::from_data(data);
        let block = DataBlock::new_from_columns(vec![col]);

        // Push the block
        output.push_data(Ok(block.clone()));

        // Pull should get all 30 rows at once
        input.set_need_data();
        assert!(input.has_data());
        let pulled_block = input.pull_data().unwrap().unwrap();
        assert_eq!(pulled_block.num_rows(), 30);

        // Should have no more data
        assert!(!input.has_data());

        // Trying to pull again should return None
        let result = input.pull_data();
        assert!(result.is_none());
    }

    Ok(())
}
