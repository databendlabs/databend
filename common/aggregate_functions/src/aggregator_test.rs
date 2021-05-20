// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_arrow::arrow::array::ArrayRef;
use common_exception::Result;

#[test]
fn test_Aggregate_function() -> Result<()> {
    use std::sync::Arc;

    use common_datablocks::DataBlock;
    use common_datavalues::*;
    use pretty_assertions::assert_eq;

    use crate::*;

    #[allow(dead_code)]
    struct Test {
        name: &'static str,
        eval_nums: usize,
        types: Vec<DataType>,
        display: &'static str,
        nullable: bool,
        columns: Vec<DataColumnarValue>,
        expect: DataValue,
        error: &'static str,
        func: Box<dyn IAggreagteFunction>
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![4, 3, 2, 1])),
        Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
    ];
    let columns = columns
        .iter()
        .map(|a| a.clone().into())
        .collect::<Vec<DataColumnarValue>>();

    let tests = vec![
        Test {
            name: "count-passed",
            eval_nums: 1,
            types: vec![DataType::Int64, DataType::Int64],
            display: "count(a)",
            nullable: false,
            func: AggregateCountFunction::try_create("count")?,
            columns: columns.clone(),
            expect: DataValue::UInt64(Some(4)),
            error: ""
        },
        Test {
            name: "max-passed",
            eval_nums: 2,
            types: vec![DataType::Int64, DataType::Int64],
            display: "max(a)",
            nullable: false,
            func: AggregateMaxFunction::try_create("max")?,
            columns: columns.clone(),
            expect: DataValue::Int64(Some(4)),
            error: ""
        },
        Test {
            name: "min-passed",
            eval_nums: 2,
            types: vec![DataType::Int64, DataType::Int64],
            display: "min(a)",
            nullable: false,
            func: AggregateMinFunction::try_create("min")?,
            columns: columns.clone(),
            expect: DataValue::Int64(Some(1)),
            error: ""
        },
        Test {
            name: "avg-passed",
            eval_nums: 1,
            types: vec![DataType::Int64, DataType::Int64],
            display: "avg(a)",
            nullable: false,
            func: AggregateAvgFunction::try_create("avg")?,
            columns: columns.clone(),
            expect: DataValue::Float64(Some(2.5)),
            error: ""
        },
        Test {
            name: "sum-passed",
            eval_nums: 1,
            types: vec![DataType::Int64, DataType::Int64],
            display: "sum(a)",
            nullable: false,
            func: AggregateSumFunction::try_create("sum")?,
            columns: columns.clone(),
            expect: DataValue::Int64(Some(10)),
            error: ""
        },
    ];

    for t in tests {
        let rows = t.columns[0].len();

        let mut func1 = t.func.clone();
        for _ in 0..t.eval_nums {
            func1.accumulate(&t.columns, rows)?;
        }
        let state1 = func1.accumulate_result()?;

        let mut func2 = t.func.clone();
        for _ in 1..t.eval_nums {
            func2.accumulate(&t.columns, rows)?;
        }
        let state2 = func2.accumulate_result()?;

        let mut final_func = t.func.clone();
        final_func.set_depth(0);
        final_func.merge(&*state1)?;
        final_func.merge(&*state2)?;

        let result = final_func.merge_result()?;

        assert_eq!(&t.expect, &result);
        assert_eq!(t.display, format!("{:}", final_func));
    }
    Ok(())
}
