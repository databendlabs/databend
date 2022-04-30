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

use std::sync::Arc;

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::bitmap::MutableBitmap;
use common_datavalues::combine_validities;
use common_datavalues::combine_validities_2;
use common_datavalues::remove_nullable;
use common_datavalues::wrap_nullable;
use common_datavalues::Column;
use common_datavalues::ColumnRef;
use common_datavalues::ConstColumn;
use common_datavalues::DataType;
use common_datavalues::DataTypeImpl;
use common_datavalues::NullColumn;
use common_datavalues::NullType;
use common_datavalues::NullableColumn;
use common_datavalues::Series;
use common_datavalues::TypeID;
use common_exception::Result;

use super::Function;
use super::FunctionDescription;
use super::Monotonicity;
use crate::scalars::FunctionContext;

#[derive(Clone)]
pub struct FunctionAdapter {
    inner: Option<Box<dyn Function>>,
    has_nullable: bool,
}

impl FunctionAdapter {
    pub fn create(inner: Box<dyn Function>, has_nullable: bool) -> Box<dyn Function> {
        Box::new(Self {
            inner: Some(inner),
            has_nullable,
        })
    }

    pub fn try_create(
        desc: &FunctionDescription,
        name: &str,
        args: &[&DataTypeImpl],
    ) -> Result<Box<dyn Function>> {
        let (inner, has_nullable) = if desc.features.passthrough_null {
            // one is null, result is null
            if args.iter().any(|v| v.data_type_id() == TypeID::Null) {
                return Ok(Box::new(Self {
                    inner: None,
                    has_nullable: false,
                }));
            }

            let has_nullable = args.iter().any(|v| v.is_nullable());
            let types = args.iter().map(|v| remove_nullable(v)).collect::<Vec<_>>();
            let types = types.iter().collect::<Vec<_>>();
            ((desc.function_creator)(name, &types)?, has_nullable)
        } else {
            ((desc.function_creator)(name, args)?, false)
        };

        Ok(Self::create(inner, has_nullable))
    }
}

impl Function for FunctionAdapter {
    fn name(&self) -> &str {
        self.inner.as_ref().map_or("null", |v| v.name())
    }

    fn return_type(&self) -> DataTypeImpl {
        if self.inner.is_none() {
            return NullType::arc();
        }

        let inner = self.inner.as_ref().unwrap();
        let typ = inner.return_type();

        if self.has_nullable {
            wrap_nullable(&typ)
        } else {
            typ
        }
    }

    fn eval(
        &self,
        func_ctx: FunctionContext,
        columns: &[ColumnRef],
        input_rows: usize,
    ) -> Result<ColumnRef> {
        if self.inner.is_none() {
            return Ok(Arc::new(NullColumn::new(input_rows)));
        }

        let inner = self.inner.as_ref().unwrap();
        if columns.is_empty() {
            return inner.eval(func_ctx, columns, input_rows);
        }

        // is there nullable constant? Did not consider this case
        // unwrap constant
        if self.passthrough_constant() && columns.iter().all(|v| v.is_const()) {
            let columns = columns
                .iter()
                .map(|c| {
                    let c: &ConstColumn = unsafe { Series::static_cast(c) };

                    c.inner().clone()
                })
                .collect::<Vec<_>>();

            let col = self.eval(func_ctx, &columns, 1)?;
            let col = if col.is_const() && col.len() != input_rows {
                col.replicate(&[input_rows])
            } else if col.is_null() {
                NullColumn::new(input_rows).arc()
            } else {
                ConstColumn::new(col, input_rows).arc()
            };
            return Ok(col);
        }

        // nullable
        if self.has_nullable && columns.iter().any(|v| v.data_type().is_nullable()) {
            let mut validity: Option<Bitmap> = None;

            let mut input = Vec::with_capacity(columns.len());
            for v in columns.iter() {
                let (is_all_null, valid) = v.validity();
                if is_all_null {
                    // If only null, return null directly.
                    let inner_type = remove_nullable(&inner.return_type());
                    return Ok(NullableColumn::wrap_inner(
                        inner_type
                            .create_constant_column(&inner_type.default_value(), input_rows)?,
                        Some(valid.unwrap().clone()),
                    ));
                }
                validity = combine_validities_2(validity.clone(), valid.cloned());

                let col = Series::remove_nullable(v);
                input.push(col);
            }

            let col = self.eval(func_ctx, &input, input_rows)?;

            // The'try' series functions always return Null when they failed the try.
            // For example, try_inet_aton("helloworld") will return Null because it failed to parse "helloworld" to a valid IP address.
            // The same thing may happen on other 'try' functions. So we need to merge the validity.
            if col.is_nullable() {
                let (_, bitmap) = col.validity();
                validity = validity.map_or(combine_validities(bitmap, None), |v| {
                    combine_validities(bitmap, Some(&v))
                })
            }

            let validity = validity.unwrap_or({
                let mut v = MutableBitmap::with_capacity(input_rows);
                v.extend_constant(input_rows, true);
                v.into()
            });

            let col = if col.is_nullable() {
                let nullable_column: &NullableColumn = Series::check_get(&col)?;
                NullableColumn::wrap_inner(nullable_column.inner().clone(), Some(validity))
            } else {
                NullableColumn::wrap_inner(col, Some(validity))
            };
            return Ok(col);
        }

        inner.eval(func_ctx, columns, input_rows)
    }

    fn get_monotonicity(&self, args: &[Monotonicity]) -> Result<Monotonicity> {
        self.inner
            .as_ref()
            .map_or(Ok(Monotonicity::create_constant()), |v| {
                v.get_monotonicity(args)
            })
    }

    fn passthrough_constant(&self) -> bool {
        self.inner
            .as_ref()
            .map_or(true, |v| v.passthrough_constant())
    }
}

impl std::fmt::Display for FunctionAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if let Some(inner) = &self.inner {
            write!(f, "{}", inner.name())
        } else {
            write!(f, "null")
        }
    }
}
