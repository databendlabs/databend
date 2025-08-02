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
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::connect;
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

        connect(&input, &output);
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

        connect(&input, &output);
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

        connect(&input, &output);

        output.finish();
        assert!(input.is_finished());
        input.set_need_data();
        assert!(input.is_finished());
    }
    Ok(())
}

mod test_test_input_and_output_with_limit {
    use std::sync::Arc;

    use databend_common_base::runtime::DataBlockLimit;
    use databend_common_base::runtime::ThreadTracker;
    use databend_common_exception::Result;
    use databend_common_expression::types::Int32Type;
    use databend_common_expression::DataBlock;
    use databend_common_expression::FromData;
    use databend_common_pipeline_core::processors::connect;
    use databend_common_pipeline_core::processors::InputPort;
    use databend_common_pipeline_core::processors::OutputPort;

    fn round_trip(input: Arc<InputPort>, output: Arc<OutputPort>) -> Result<usize> {
        input.set_need_data();
        assert!(output.can_push());
        let col = Int32Type::from_data(vec![1, 2, 3, 4, 5]);
        let block = DataBlock::new_from_columns(vec![col.clone()]);
        output.push_data(Ok(block));
        assert!(input.has_data());
        let data_block = input.pull_data().unwrap()?;
        Ok(data_block.num_rows())
    }

    fn test_with_datablock_limit(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        limit: Option<DataBlockLimit>,
    ) -> Result<usize> {
        let mut payload = ThreadTracker::new_tracking_payload();
        payload.datablock_limit = limit.map(Arc::new);
        let _guard = ThreadTracker::tracking(payload);
        round_trip(input, output)
    }

    #[test]
    fn test_input_and_output_with_limit_basic() -> Result<()> {
        unsafe {
            let input = InputPort::create();
            let output = OutputPort::create();
            connect(&input, &output);

            // Test 1: No limit - should return all 5 rows
            let rows = test_with_datablock_limit(input.clone(), output.clone(), None)?;
            assert_eq!(rows, 5);

            // Test 2: Limit > data size - should return all 5 rows
            let limit = DataBlockLimit::new(100, usize::MAX);
            let rows = test_with_datablock_limit(input.clone(), output.clone(), Some(limit))?;
            assert_eq!(rows, 5);

            // Test 3: Limit = data size - should return all 5 rows
            let limit = DataBlockLimit::new(5, usize::MAX);
            let rows = test_with_datablock_limit(input.clone(), output.clone(), Some(limit))?;
            assert_eq!(rows, 5);

            // Test 4: Limit < data size - should return limited rows and leave remaining data
            let limit = DataBlockLimit::new(2, usize::MAX);
            let rows = test_with_datablock_limit(input.clone(), output.clone(), Some(limit))?;
            assert_eq!(rows, 2);
            assert!(input.has_data());

            // Test 5: Pull remaining data after limit
            {
                let mut payload = ThreadTracker::new_tracking_payload();
                let limit = DataBlockLimit::new(100, usize::MAX);
                payload.datablock_limit = Some(Arc::new(limit));
                let _guard = ThreadTracker::tracking(payload);
                let data_block = input.pull_data().unwrap()?;
                assert_eq!(data_block.num_rows(), 3);
                assert!(!input.has_data());
                assert!(!input.is_need_data());
            }

            // Test 6: Byte limit test - estimate block size for 5 rows of Int32
            // Each Int32 takes 4 bytes, so 5 * 4 = 20 bytes
            let limit = DataBlockLimit::new(usize::MAX, 10); // Allow only 10 bytes
            let rows = test_with_datablock_limit(input.clone(), output.clone(), Some(limit))?;
            // 10 bytes / 4 bytes per row = 2.5, so we get 2 rows
            assert_eq!(rows, 2);
            assert!(input.has_data());

            // Test 7: Pull remaining data after byte limit
            {
                let mut payload = ThreadTracker::new_tracking_payload();
                let limit = DataBlockLimit::new(100, usize::MAX);
                payload.datablock_limit = Some(Arc::new(limit));
                let _guard = ThreadTracker::tracking(payload);
                let data_block = input.pull_data().unwrap()?;
                assert_eq!(data_block.num_rows(), 3);
                assert!(!input.has_data());
                assert!(!input.is_need_data());
            }

            // Test 8: Test byte limit that forces minimum 1 row
            // Set byte limit to 2 bytes (less than 1 row)
            let limit = DataBlockLimit::new(usize::MAX, 2); // Allow only 2 bytes
            let rows = test_with_datablock_limit(input.clone(), output.clone(), Some(limit))?;
            // Should return at least 1 row due to max(1) in rows_to_take
            assert_eq!(rows, 1);
            assert!(input.has_data());

            // Clean up remaining data
            {
                let mut payload = ThreadTracker::new_tracking_payload();
                let limit = DataBlockLimit::new(100, usize::MAX);
                payload.datablock_limit = Some(Arc::new(limit));
                let _guard = ThreadTracker::tracking(payload);
                let data_block = input.pull_data().unwrap()?;
                assert_eq!(data_block.num_rows(), 4);
            }

            // Test 9: Both row and byte limits - row limit is more restrictive
            let limit = DataBlockLimit::new(3, 1000); // 3 rows, 1000 bytes
            let rows = test_with_datablock_limit(input.clone(), output.clone(), Some(limit))?;
            assert_eq!(rows, 3);
            assert!(input.has_data());

            // Clean up remaining data
            {
                let mut payload = ThreadTracker::new_tracking_payload();
                let limit = DataBlockLimit::new(100, usize::MAX);
                payload.datablock_limit = Some(Arc::new(limit));
                let _guard = ThreadTracker::tracking(payload);
                let data_block = input.pull_data().unwrap()?;
                assert_eq!(data_block.num_rows(), 2);
            }
        }

        Ok(())
    }
}
