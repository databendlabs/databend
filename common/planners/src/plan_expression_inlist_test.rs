// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use pretty_assertions::assert_eq;
use crate::*;

#[cfg(test)]
mod tests {
    // applies the in_list expr to an input batch and list
    macro_rules! in_list {
        ($BATCH:expr, $LIST:expr, $NEGATED:expr, $EXPECTED:expr, $COL:expr) => {{
            let expr = in_list($COL, $LIST, $NEGATED).unwrap();
            let result = expr.evaluate(&$BATCH)?.into_array($BATCH.num_rows());
            let result = result
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("failed to downcast to BooleanArray");
            let expected = &BooleanArray::from($EXPECTED);
            assert_eq!(expected, result);
        }};
    }

    #[test]
    fn in_list_utf8() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, true)]);
        let a = StringArray::from(vec![Some("a"), Some("d"), None]);
        let col_a = col("a", &schema)?;
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)])?;

        // expression: "a in ("a", "b")"
        let list = vec![
            lit(ScalarValue::Utf8(Some("a".to_string()))),
            lit(ScalarValue::Utf8(Some("b".to_string()))),
        ];
        in_list!(
            batch,
            list,
            &false,
            vec![Some(true), Some(false), None],
            col_a.clone()
        );

        // expression: "a not in ("a", "b")"
        let list = vec![
            lit(ScalarValue::Utf8(Some("a".to_string()))),
            lit(ScalarValue::Utf8(Some("b".to_string()))),
        ];
        in_list!(
            batch,
            list,
            &true,
            vec![Some(false), Some(true), None],
            col_a.clone()
        );

        // expression: "a not in ("a", "b")"
        let list = vec![
            lit(ScalarValue::Utf8(Some("a".to_string()))),
            lit(ScalarValue::Utf8(Some("b".to_string()))),
            lit(ScalarValue::Utf8(None)),
        ];
        in_list!(
            batch,
            list,
            &false,
            vec![Some(true), None, None],
            col_a.clone()
        );

        // expression: "a not in ("a", "b")"
        let list = vec![
            lit(ScalarValue::Utf8(Some("a".to_string()))),
            lit(ScalarValue::Utf8(Some("b".to_string()))),
            lit(ScalarValue::Utf8(None)),
        ];
        in_list!(
            batch,
            list,
            &true,
            vec![Some(false), None, None],
            col_a.clone()
        );

        Ok(())
    }
}

