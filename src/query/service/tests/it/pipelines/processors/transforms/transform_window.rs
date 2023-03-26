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

use std::sync::Arc;

use common_base::base::tokio;
use common_exception::Result;
use common_expression::block_debug::assert_blocks_eq;
use common_expression::types::DataType;
use common_expression::types::Int32Type;
use common_expression::types::NumberDataType;
use common_expression::DataBlock;
use common_expression::FromData;
use common_functions::aggregates::AggregateFunctionFactory;
use common_pipeline_core::processors::connect;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::Processor;
use databend_query::pipelines::processors::TransformWindow;
use databend_query::pipelines::processors::WindowFrame;
use databend_query::pipelines::processors::WindowFrameBound;

#[allow(clippy::type_complexity)]
fn get_transform_window(
    window_frame: WindowFrame,
) -> Result<(Box<dyn Processor>, Arc<InputPort>, Arc<OutputPort>)> {
    let function = AggregateFunctionFactory::instance()
        .get("sum", vec![], vec![DataType::Number(NumberDataType::Int32)])?;
    let input = InputPort::create();
    let output = OutputPort::create();
    let transform = TransformWindow::try_create(
        input.clone(),
        output.clone(),
        function,
        vec![0],
        vec![0],
        window_frame,
    )?;

    Ok((Box::new(transform), input, output))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_transform_window() -> Result<()> {
    {
        let upstream_output = OutputPort::create();
        let downstream_input = InputPort::create();
        let (mut transform, input, output) = get_transform_window(WindowFrame {
            start_bound: WindowFrameBound::Preceding(Some(1)),
            end_bound: WindowFrameBound::Following(Some(1)),
        })?;

        unsafe {
            connect(&input, &upstream_output);
            connect(&downstream_input, &output);
        }

        downstream_input.set_need_data();

        assert!(matches!(transform.event()?, Event::NeedData));
        upstream_output.push_data(Ok(DataBlock::new_from_columns(vec![Int32Type::from_data(
            vec![1, 1, 1, 2, 2, 3, 3, 3],
        )])));

        assert!(matches!(transform.event()?, Event::Sync));
        transform.process()?;

        assert!(matches!(transform.event()?, Event::NeedData));
        upstream_output.push_data(Ok(DataBlock::new_from_columns(vec![Int32Type::from_data(
            vec![3, 4, 4],
        )])));
        upstream_output.finish();

        assert!(matches!(transform.event()?, Event::Sync));
        transform.process()?;

        assert!(matches!(transform.event()?, Event::NeedConsume));
        let block = downstream_input.pull_data().unwrap()?;
        assert_blocks_eq(
            vec![
                "+----------+----------+",
                "| Column 0 | Column 1 |",
                "+----------+----------+",
                "| 1        | 2        |",
                "| 1        | 3        |",
                "| 1        | 2        |",
                "| 2        | 4        |",
                "| 2        | 4        |",
                "| 3        | 6        |",
                "| 3        | 9        |",
                "| 3        | 9        |",
                "+----------+----------+",
            ],
            &[block],
        );
        downstream_input.set_need_data();

        assert!(matches!(transform.event()?, Event::Sync));
        transform.process()?;

        assert!(matches!(transform.event()?, Event::NeedConsume));
        let block = downstream_input.pull_data().unwrap()?;
        assert_blocks_eq(
            vec![
                "+----------+----------+",
                "| Column 0 | Column 1 |",
                "+----------+----------+",
                "| 3        | 6        |",
                "| 4        | 8        |",
                "| 4        | 8        |",
                "+----------+----------+",
            ],
            &[block],
        );
        downstream_input.set_need_data();

        assert!(matches!(transform.event()?, Event::Finished));
    }

    {
        let upstream_output = OutputPort::create();
        let downstream_input = InputPort::create();
        let (mut transform, input, output) = get_transform_window(WindowFrame {
            start_bound: WindowFrameBound::Preceding(None),
            end_bound: WindowFrameBound::Following(None),
        })?;

        unsafe {
            connect(&input, &upstream_output);
            connect(&downstream_input, &output);
        }

        downstream_input.set_need_data();

        assert!(matches!(transform.event()?, Event::NeedData));
        upstream_output.push_data(Ok(DataBlock::new_from_columns(vec![Int32Type::from_data(
            vec![1, 1, 1],
        )])));

        assert!(matches!(transform.event()?, Event::Sync));
        transform.process()?;

        assert!(matches!(transform.event()?, Event::NeedData));
        upstream_output.push_data(Ok(DataBlock::new_from_columns(vec![Int32Type::from_data(
            vec![1, 1, 1],
        )])));

        assert!(matches!(transform.event()?, Event::Sync));
        transform.process()?;

        assert!(matches!(transform.event()?, Event::NeedData));
        upstream_output.push_data(Ok(DataBlock::new_from_columns(vec![Int32Type::from_data(
            vec![1, 1, 1],
        )])));
        upstream_output.finish();

        assert!(matches!(transform.event()?, Event::Sync));
        transform.process()?;

        assert!(matches!(transform.event()?, Event::Sync));
        transform.process()?;

        assert!(matches!(transform.event()?, Event::NeedConsume));
        let block = downstream_input.pull_data().unwrap()?;
        assert_blocks_eq(
            vec![
                "+----------+----------+",
                "| Column 0 | Column 1 |",
                "+----------+----------+",
                "| 1        | 9        |",
                "| 1        | 9        |",
                "| 1        | 9        |",
                "+----------+----------+",
            ],
            &[block],
        );
        downstream_input.set_need_data();

        assert!(matches!(transform.event()?, Event::NeedConsume));
        let block = downstream_input.pull_data().unwrap()?;
        assert_blocks_eq(
            vec![
                "+----------+----------+",
                "| Column 0 | Column 1 |",
                "+----------+----------+",
                "| 1        | 9        |",
                "| 1        | 9        |",
                "| 1        | 9        |",
                "+----------+----------+",
            ],
            &[block],
        );
        downstream_input.set_need_data();

        assert!(matches!(transform.event()?, Event::NeedConsume));
        let block = downstream_input.pull_data().unwrap()?;
        assert_blocks_eq(
            vec![
                "+----------+----------+",
                "| Column 0 | Column 1 |",
                "+----------+----------+",
                "| 1        | 9        |",
                "| 1        | 9        |",
                "| 1        | 9        |",
                "+----------+----------+",
            ],
            &[block],
        );
        downstream_input.set_need_data();

        assert!(matches!(transform.event()?, Event::Finished));
    }

    Ok(())
}
