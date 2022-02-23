// Copyright 2021 Datafuse Labs.
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

use std::fmt;
use std::ops::AddAssign;

use common_datavalues::prelude::*;
use common_datavalues::with_match_primitive_type_id;
use common_exception::Result;
use num_format::Locale;
use num_format::ToFormattedString;
use num_traits::AsPrimitive;

use crate::scalars::assert_numeric;
use crate::scalars::assert_string;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::EvalContext;
use crate::scalars::Function;
use crate::scalars::FunctionDescription;
use crate::scalars::ScalarBinaryExpression;

const FORMAT_MAX_DECIMALS: i64 = 30;

#[derive(Clone)]
pub struct FormatFunction {
    _display_name: String,
}

// FORMAT(X,D[,locale])
// Formats the number X to a format like '#,###,###.##', rounded to D decimal places, and returns the result as a string.
// If D is 0, the result has no decimal point or fractional part.
impl FormatFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(FormatFunction {
            _display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create)).features(
            FunctionFeatures::default()
                .deterministic()
                .variadic_arguments(2, 3),
        )
    }
}

impl Function for FormatFunction {
    fn name(&self) -> &str {
        "format"
    }

    fn return_type(&self, args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        assert_numeric(args[0])?;
        assert_numeric(args[1])?;
        if args.len() >= 3 {
            assert_string(args[2])?;
        }
        Ok(Vu8::to_data_type())
    }

    fn eval(&self, columns: &ColumnsWithField, _input_rows: usize) -> Result<ColumnRef> {
        with_match_primitive_type_id!(columns[0].data_type().data_type_id(), |$F| {
                with_match_primitive_type_id!(columns[1].data_type().data_type_id(), |$N| {
                    let binary = ScalarBinaryExpression::<$F, $N, Vu8, _>::new(format_en_us);
                    let col = binary.eval(columns[0].column(), columns[1].column(), &mut EvalContext::default())?;
                    Ok(col.arc())
                },{
                    unreachable!()

                })
        },{
            unreachable!()
        })
    }
}

fn format_en_us<L, R>(number: L, precision: R, _ctx: &mut EvalContext) -> Vec<u8>
where
    L: AsPrimitive<f64> + AsPrimitive<i64>,
    R: AsPrimitive<i64>,
{
    let precision = precision.as_();
    let precision = if precision > FORMAT_MAX_DECIMALS {
        FORMAT_MAX_DECIMALS
    } else if precision < 0 {
        0
    } else {
        precision
    };

    let trunc: i64 = number.as_();
    let number: f64 = number.as_();
    let fract = (number - trunc as f64).abs();
    let fract_str = format!("{0:.1$}", fract, precision as usize);
    let fract_str = fract_str.strip_prefix('0');
    let mut trunc_str = trunc.to_formatted_string(&Locale::en);
    if let Some(s) = fract_str {
        trunc_str.add_assign(s)
    }
    Vec::from(trunc_str)
}

impl fmt::Display for FormatFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "FORMAT")
    }
}
