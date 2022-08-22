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

#[macro_export]
macro_rules! calcute {
    ($lhs_viewer: expr, $rhs_viewer: expr, $builder: expr, $func: expr) => {
        for (a, b) in ($lhs_viewer.iter().zip($rhs_viewer.iter())) {
            $builder.append($func(a, b));
        }
    };
}

#[macro_export]
macro_rules! impl_logic_expression {
    ($name: ident, $method: tt, $func: expr) => {
        #[derive(Clone)]
        pub struct $name;

        impl LogicExpression for $name {
            fn eval(func_ctx: FunctionContext, columns: &ColumnsWithField, input_rows: usize, nullable: bool) -> Result<ColumnRef> {
                let dt = if nullable {
                    NullableType::new_impl(BooleanType::new_impl())
                } else {
                    BooleanType::new_impl()
                };

                let lhs = cast_column_field(&columns[0], columns[0].data_type(), &dt, &func_ctx)?;
                let rhs = cast_column_field(&columns[1], columns[1].data_type(), &dt, &func_ctx)?;

                if nullable {
                    let lhs_viewer = bool::try_create_viewer(&lhs)?;
                    let rhs_viewer = bool::try_create_viewer(&rhs)?;

                    let lhs_viewer_iter = lhs_viewer.iter();
                    let rhs_viewer_iter = rhs_viewer.iter();

                    let mut builder = NullableColumnBuilder::<bool>::with_capacity_meta(input_rows, lhs.column_meta());

                    for (a, (idx, b)) in lhs_viewer_iter.zip(rhs_viewer_iter.enumerate()) {
                        let (val, valid) = $func(a, b, lhs_viewer.valid_at(idx), rhs_viewer.valid_at(idx));
                        builder.append(val, valid);
                    }

                    Ok(builder.build(input_rows))
                } else {
                    let lhs_viewer = bool::try_create_viewer(&lhs)?;
                    let rhs_viewer = bool::try_create_viewer(&rhs)?;

                    let mut builder = ColumnBuilder::<bool>::with_capacity(input_rows);

                    calcute!(lhs_viewer, rhs_viewer, builder, |lhs: bool,
                                                               rhs: bool|
                     -> bool {
                        lhs $method rhs
                    });

                    Ok(builder.build(input_rows))
                }
            }
        }
    };
}
