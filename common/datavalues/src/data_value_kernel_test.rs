// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[test]
fn test_data_value_kernel_concat_row_key() -> anyhow::Result<()> {
    use std::sync::Arc;

    use pretty_assertions::assert_eq;

    use super::*;

    #[allow(dead_code)]
    struct ArrayTest {
        name: &'static str,
        args: Vec<DataArrayRef>,
        expect: Vec<&'static str>,
        error: Vec<&'static str>,
    }

    let tests = vec![ArrayTest {
        name: "passed",
        args: vec![
            Arc::new(StringArray::from(vec!["x1", "x2"])),
            Arc::new(Int8Array::from(vec![1, 2])),
            Arc::new(Int16Array::from(vec![4, 3, ])),
            Arc::new(Int32Array::from(vec![4, 3, ])),
            Arc::new(Int64Array::from(vec![4, 3, ])),
            Arc::new(UInt8Array::from(vec![4, 3, ])),
            Arc::new(UInt16Array::from(vec![4, 3,])),
            Arc::new(UInt32Array::from(vec![4, 3,])),
            Arc::new(UInt64Array::from(vec![4, 3, ])),
            Arc::new(Float32Array::from(vec![4.0, 3.0])),
            Arc::new(Float64Array::from(vec![4.0, 3.0])),
        ],
        expect: vec!["[2, 0, 0, 0, 0, 0, 0, 0, 120, 49, 1, 4, 0, 4, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 4, 4, 0, 4, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128, 64, 0, 0, 0, 0, 0, 0, 16, 64]", 
                     "[2, 0, 0, 0, 0, 0, 0, 0, 120, 50, 2, 3, 0, 3, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 3, 3, 0, 3, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 64, 64, 0, 0, 0, 0, 0, 0, 8, 64]", 
        ],
        error: vec![""],
    }];

    for t in tests {
        for row in 0..2 {
            let mut key: Vec<u8> = vec![];
            for col in 0..t.args.len() {
                DataValue::concat_row_to_one_key(&t.args[col], row, &mut key)?;
            }
            assert_eq!(format!("{:?}", key), t.expect[row]);
        }
    }

    Ok(())
}
