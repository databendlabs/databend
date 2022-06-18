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

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_datavalues::remove_nullable;
use common_datavalues::type_coercion::aggregate_types;
use common_datavalues::with_match_scalar_type;
use common_exception::ErrorCode;
use common_exception::Result;

use super::IfFunction;
use crate::scalars::cast_column_field;
use crate::scalars::Function;
use crate::scalars::FunctionContext;
use crate::scalars::FunctionDescription;
use crate::scalars::FunctionFeatures;

#[derive(Clone, Debug)]
pub struct MultiIfFunction {
    display_name: String,
    return_type: DataTypeImpl,
}

impl MultiIfFunction {
    pub fn try_create(display_name: &str, args: &[&DataTypeImpl]) -> Result<Box<dyn Function>> {
        if args.len() == 3 {
            return IfFunction::try_create(display_name, args);
        }

        if !(args.len() >= 3 && args.len() % 2 == 1) {
            return Err(ErrorCode::NumberArgumentsNotMatch(
                "Invalid number of arguments for function multi_if".to_string(),
            ));
        }

        let mut type_of_branches = Vec::with_capacity(args.len() / 2);
        // branch
        for i in (1..args.len()).step_by(2) {
            type_of_branches.push(args[i].clone());
        }
        type_of_branches.push(args[args.len() - 1].clone());

        let return_type = aggregate_types(type_of_branches.as_slice())?;
        Ok(Box::new(MultiIfFunction {
            display_name: display_name.to_string(),
            return_type,
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create)).features(
            FunctionFeatures::default()
                .deterministic()
                .disable_passthrough_null()
                .variadic_arguments(3, usize::MAX),
        )
    }

    fn eval_nonull<S: Scalar>(
        condition_columns: Vec<ColumnRef>,
        source_columns: Vec<ColumnRef>,
        input_rows: usize,
    ) -> Result<ColumnRef> {
        let condition_viewers = condition_columns
            .iter()
            .map(BooleanViewer::try_create)
            .collect::<Result<Vec<BooleanViewer>>>()?;
        let source_viewers = source_columns
            .iter()
            .map(S::try_create_viewer)
            .collect::<Result<Vec<S::Viewer<'_>>>>()?;
        let mut builder =
            ColumnBuilder::<S>::with_capacity_meta(input_rows, source_columns[0].column_meta());

        for row in 0..input_rows {
            for (idx, cv) in condition_viewers.iter().enumerate() {
                if cv.value_at(row) {
                    builder.append(source_viewers[idx].value_at(row));
                    break;
                } else {
                    // last
                    if idx == condition_viewers.len() - 1 {
                        builder.append(source_viewers[idx + 1].value_at(row));
                    }
                }
            }
        }
        Ok(builder.build(input_rows))
    }

    fn eval_nullable<S: Scalar>(
        condition_columns: Vec<ColumnRef>,
        source_columns: Vec<ColumnRef>,
        input_rows: usize,
    ) -> Result<ColumnRef> {
        let condition_viewers = condition_columns
            .iter()
            .map(BooleanViewer::try_create)
            .collect::<Result<Vec<BooleanViewer>>>()?;
        let source_viewers = source_columns
            .iter()
            .map(S::try_create_viewer)
            .collect::<Result<Vec<S::Viewer<'_>>>>()?;
        let mut builder = NullableColumnBuilder::<S>::with_capacity_meta(
            input_rows,
            source_columns[0].column_meta(),
        );

        for row in 0..input_rows {
            for (idx, cv) in condition_viewers.iter().enumerate() {
                if cv.value_at(row) {
                    builder.append(
                        source_viewers[idx].value_at(row),
                        source_viewers[idx].valid_at(row),
                    );
                    break;
                } else {
                    // last
                    if idx == condition_viewers.len() - 1 {
                        builder.append(
                            source_viewers[idx + 1].value_at(row),
                            source_viewers[idx + 1].valid_at(row),
                        );
                    }
                }
            }
        }
        Ok(builder.build(input_rows))
    }
}

impl Function for MultiIfFunction {
    fn name(&self) -> &str {
        "MultiIfFunction"
    }

    fn return_type(&self) -> DataTypeImpl {
        self.return_type.clone()
    }

    // multi_if(c1, a1, c2, a2, c3, a3, c4, a4, d)
    fn eval(
        &self,
        func_ctx: FunctionContext,
        columns: &ColumnsWithField,
        input_rows: usize,
    ) -> Result<ColumnRef> {
        let mut condition_columns = Vec::with_capacity(columns.len() / 2);
        // branch
        for i in (0..columns.len() - 1).step_by(2) {
            let cond_col = DataBlock::cast_to_nonull_boolean(columns[i].column())?;
            condition_columns.push(cond_col);
        }

        // value
        let mut source_columns = Vec::with_capacity(columns.len() / 2);
        for i in (1..columns.len()).step_by(2) {
            let source = cast_column_field(
                &columns[i],
                columns[i].data_type(),
                &self.return_type,
                &func_ctx,
            )?;
            source_columns.push(source);
        }
        let source_last = cast_column_field(
            &columns[columns.len() - 1],
            columns[columns.len() - 1].data_type(),
            &self.return_type,
            &func_ctx,
        )?;
        source_columns.push(source_last);

        if self.return_type.is_nullable() {
            let inner_type = remove_nullable(&self.return_type);
            with_match_scalar_type!(inner_type.data_type_id().to_physical_type(), |$T| {
                Self::eval_nullable::<$T>(condition_columns, source_columns, input_rows)
            }, {
                Err(ErrorCode::BadDataValueType(format!(
                    "Column with type: {:?} does not support multi_if",
                    self.return_type
                )))
            })
        } else {
            with_match_scalar_type!(self.return_type.data_type_id().to_physical_type(), |$T| {
                Self::eval_nonull::<$T>(condition_columns, source_columns, input_rows)
            }, {
                Err(ErrorCode::BadDataValueType(format!(
                    "Column with type: {:?} does not support multi_if",
                    self.return_type
                )))
            })
        }
    }
}

impl std::fmt::Display for MultiIfFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}()", self.display_name)
    }
}
