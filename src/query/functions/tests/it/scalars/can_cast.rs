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

use databend_common_exception::Result;
use databend_common_expression::date_helper::TzLUT;
use databend_common_expression::type_check::can_cast_to;
use databend_common_expression::type_check::check_cast;
use databend_common_expression::types::variant::cast_scalar_to_variant;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DecimalDataType;
use databend_common_expression::types::DecimalSize;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::Expr::ColumnRef;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::Value;
use databend_common_functions::BUILTIN_FUNCTIONS;
use roaring::RoaringTreemap;

fn cast_simple_value(src_type: &DataType, dest_type: &DataType) -> Result<Expr> {
    let n = 2;
    let src_str = match dest_type {
        DataType::Boolean => "false",
        DataType::Timestamp | DataType::Date => "1970-01-01 00:00:00.000000",
        _ => "0",
    };
    let value = match src_type {
        DataType::String => Value::Column(
            ColumnBuilder::repeat(&Scalar::String(src_str.to_string()).as_ref(), n, src_type)
                .build(),
        ),
        DataType::Binary => Value::Column(
            ColumnBuilder::repeat(
                &Scalar::Binary(src_str.as_bytes().to_vec()).as_ref(),
                n,
                src_type,
            )
            .build(),
        ),
        DataType::Variant => {
            let s = Scalar::String(src_str.to_string());
            let mut buf = vec![];
            cast_scalar_to_variant(s.as_ref(), TzLUT::default(), &mut buf);
            let s = Scalar::Variant(buf);
            Value::Column(ColumnBuilder::repeat(&s.as_ref(), n, src_type).build())
        }
        _ => Value::Column(ColumnBuilder::repeat_default(src_type, n).build()),
    };

    let block = DataBlock::new(vec![BlockEntry::new(src_type.clone(), value)], 2);
    let func_ctx = FunctionContext::default();
    let evaluator = Evaluator::new(&block, &func_ctx, &BUILTIN_FUNCTIONS);
    let expr = ColumnRef {
        span: None,
        id: 0,
        data_type: src_type.clone(),
        display_name: "c1".to_string(),
    };
    let expr = check_cast(None, false, expr, dest_type, &BUILTIN_FUNCTIONS)?;
    let r = evaluator.run(&expr)?;
    let r0 = r.index(0).unwrap();
    let exp = match dest_type {
        DataType::Boolean => Scalar::Boolean(false),
        DataType::Binary => Scalar::Binary("0".as_bytes().to_vec()),
        DataType::Timestamp => Scalar::Timestamp(0),
        DataType::Date => Scalar::Date(0),
        DataType::Bitmap => {
            let mut buf = vec![];
            let rb = RoaringTreemap::from_iter([0].iter());
            rb.serialize_into(&mut buf).unwrap();
            Scalar::Bitmap(buf)
        }
        _ => Scalar::default_value(dest_type),
    };
    assert_eq!(
        r0,
        exp.as_ref(),
        "wrong cast result: {src_type} -> {dest_type}: expect {exp:?}, got {r0:?}",
    );
    Ok(expr)
}

#[test]
fn test_can_cast_to() {
    let types = [
        DataType::Number(NumberDataType::Float32),
        DataType::Number(NumberDataType::Float64),
        DataType::Number(NumberDataType::Int8),
        DataType::Number(NumberDataType::UInt8),
        DataType::Decimal(DecimalDataType::Decimal128(DecimalSize {
            precision: 10,
            scale: 2,
        })),
        DataType::Decimal(DecimalDataType::Decimal128(DecimalSize {
            precision: 10,
            scale: 0,
        })),
        DataType::Boolean,
        DataType::Timestamp,
        DataType::Date,
        DataType::String,
        DataType::Binary,
        DataType::Variant,
        DataType::Bitmap,
        // todo: fix bug about default value of Geometry
        // DataType::Geometry,
    ];

    // evaluating
    for dst in &types {
        if !matches!(dst, DataType::String | DataType::Variant) {
            for src in &types {
                if src != dst {
                    let exp = can_cast_to(src, dst);
                    let res = cast_simple_value(src, dst);
                    if let Err(err) = &res {
                        assert!(err.message().contains("unable to cast type"), "{err}");
                        assert!(!err.message().contains("evaluating"), "{err}");
                    };
                    let res = res.is_ok();
                    assert_eq!(
                        res, exp,
                        "{src} to {dst} is {}, but can_cast_to return {exp}",
                        res
                    )
                }
            }
        }
    }
}
