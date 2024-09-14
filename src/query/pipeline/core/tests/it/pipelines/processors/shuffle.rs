// Copyright 2023 Datafuse Labs.
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

use databend_common_exception::Result;
use databend_common_expression::types::Int32Type;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_pipeline_core::processors::connect;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ShuffleProcessor;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_shuffle_output_finish() -> Result<()> {
    let input1 = InputPort::create();
    let input2 = InputPort::create();
    let output1 = OutputPort::create();
    let output2 = OutputPort::create();

    let mut processor = ShuffleProcessor::create(
        vec![input1.clone(), input2.clone()],
        vec![output1.clone(), output2.clone()],
        vec![0, 1],
    );

    let upstream_output1 = OutputPort::create();
    let upstream_output2 = OutputPort::create();
    let downstream_input1 = InputPort::create();
    let downstream_input2 = InputPort::create();

    unsafe {
        connect(&input1, &upstream_output1);
        connect(&input2, &upstream_output2);
        connect(&downstream_input1, &output1);
        connect(&downstream_input2, &output2);
    }

    downstream_input1.finish();
    downstream_input2.finish();

    assert!(matches!(processor.event()?, Event::Finished));
    assert!(input1.is_finished() && input2.is_finished());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_shuffle_processor() -> Result<()> {
    let input1 = InputPort::create();
    let input2 = InputPort::create();
    let input3 = InputPort::create();
    let input4 = InputPort::create();
    let output1 = OutputPort::create();
    let output2 = OutputPort::create();
    let output3 = OutputPort::create();
    let output4 = OutputPort::create();

    let mut processor = ShuffleProcessor::create(
        vec![
            input1.clone(),
            input2.clone(),
            input3.clone(),
            input4.clone(),
        ],
        vec![
            output1.clone(),
            output2.clone(),
            output3.clone(),
            output4.clone(),
        ],
        vec![0, 2, 1, 3],
    );

    let upstream_output1 = OutputPort::create();
    let upstream_output2 = OutputPort::create();
    let upstream_output3 = OutputPort::create();
    let upstream_output4 = OutputPort::create();
    let downstream_input1 = InputPort::create();
    let downstream_input2 = InputPort::create();
    let downstream_input3 = InputPort::create();
    let downstream_input4 = InputPort::create();

    unsafe {
        connect(&input1, &upstream_output1);
        connect(&input2, &upstream_output2);
        connect(&input3, &upstream_output3);
        connect(&input4, &upstream_output4);
        connect(&downstream_input1, &output1);
        connect(&downstream_input2, &output2);
        connect(&downstream_input3, &output3);
        connect(&downstream_input4, &output4);
    }

    let col1 = Int32Type::from_data(vec![1]);
    let col2 = Int32Type::from_data(vec![2]);
    let col3 = Int32Type::from_data(vec![3]);
    let col4 = Int32Type::from_data(vec![4]);
    let block1 = DataBlock::new_from_columns(vec![col1.clone()]);
    let block2 = DataBlock::new_from_columns(vec![col2.clone()]);
    let block3 = DataBlock::new_from_columns(vec![col3.clone()]);
    let block4 = DataBlock::new_from_columns(vec![col4.clone()]);

    downstream_input1.set_need_data();
    downstream_input2.set_need_data();
    downstream_input3.set_need_data();
    downstream_input4.set_need_data();
    upstream_output1.push_data(Ok(block1));
    upstream_output2.push_data(Ok(block2));
    upstream_output3.push_data(Ok(block3));
    upstream_output4.push_data(Ok(block4));

    assert!(matches!(processor.event()?, Event::NeedData));

    let out1 = downstream_input1.pull_data().unwrap()?;
    let out2 = downstream_input2.pull_data().unwrap()?;
    let out3 = downstream_input3.pull_data().unwrap()?;
    let out4 = downstream_input4.pull_data().unwrap()?;

    assert!(out1.columns()[0].value.as_column().unwrap().eq(&col1));
    assert!(out2.columns()[0].value.as_column().unwrap().eq(&col3));
    assert!(out3.columns()[0].value.as_column().unwrap().eq(&col2));
    assert!(out4.columns()[0].value.as_column().unwrap().eq(&col4));

    Ok(())
}
