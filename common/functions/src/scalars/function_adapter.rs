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
use common_arrow::arrow::bitmap::MutableBitmap;
use common_datavalues::combine_validities;
use common_datavalues::combine_validities_2;
use common_datavalues::remove_nullable;
use common_datavalues::wrap_nullable;
use common_datavalues::Column;
use common_datavalues::ColumnRef;
use common_datavalues::ColumnWithField;
use common_datavalues::ColumnsWithField;
use common_datavalues::ConstColumn;
use common_datavalues::DataField;
use common_datavalues::DataTypePtr;
use common_datavalues::NullColumn;
use common_datavalues::NullType;
use common_datavalues::NullableColumn;
use common_datavalues::Series;
use common_datavalues::TypeID;
use common_exception::Result;

use super::Function;
use super::Monotonicity;
use super::TypedFunctionDescription;

#[derive(Clone)]
pub struct FunctionAdapter {
    inner: Option<Box<dyn Function>>,
    passthrough_null: bool,
}

impl FunctionAdapter {
    pub fn create(inner: Box<dyn Function>, passthrough_null: bool) -> Box<dyn Function> {
        Box::new(Self {
            inner: Some(inner),
            passthrough_null,
        })
    }

    pub fn create_some(
        inner: Option<Box<dyn Function>>,
        passthrough_null: bool,
    ) -> Box<dyn Function> {
        Box::new(Self {
            inner,
            passthrough_null,
        })
    }

    pub fn try_create_by_typed(
        desc: &TypedFunctionDescription,
        name: &str,
        args: &[&DataTypePtr],
    ) -> Result<Box<dyn Function>> {
        let passthrough_null = desc.features.passthrough_null;

        let inner = if passthrough_null {
            // one is null, result is null
            if args.iter().any(|v| v.data_type_id() == TypeID::Null) {
                return Ok(Self::create_some(None, true));
            }
            let types = args.iter().map(|v| remove_nullable(v)).collect::<Vec<_>>();
            let types = types.iter().collect::<Vec<_>>();
            (desc.typed_function_creator)(name, &types)?
        } else {
            (desc.typed_function_creator)(name, args)?
        };

        Ok(Self::create(inner, passthrough_null))
    }
}

impl Function for FunctionAdapter {
    fn name(&self) -> &str {
        self.inner.as_ref().map_or("null", |v| v.name())
    }

    fn return_type(&self, args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        if self.inner.is_none() {
            return Ok(NullType::arc());
        }

        let inner = self.inner.as_ref().unwrap();

        if self.passthrough_null {
            let has_null = args.iter().any(|v| v.is_null());
            if has_null {
                return Ok(NullType::arc());
            }

            let has_nullable = args.iter().any(|v| v.is_nullable());
            let types = args.iter().map(|v| remove_nullable(v)).collect::<Vec<_>>();
            let types = types.iter().collect::<Vec<_>>();
            let typ = inner.return_type(&types)?;

            if has_nullable {
                Ok(wrap_nullable(&typ))
            } else {
                Ok(typ)
            }
        } else {
            inner.return_type(args)
        }
    }

    fn eval(
        &self,
        columns: &ColumnsWithField,
        input_rows: usize,
        func_ctx: FunctionContext,
    ) -> Result<ColumnRef> {
        if self.inner.is_none() {
            return Ok(Arc::new(NullColumn::new(input_rows)));
        }

        let inner = self.inner.as_ref().unwrap();
        if columns.is_empty() {
            return inner.eval(columns, input_rows, func_ctx);
        }

        // is there nullable constant? Did not consider this case
        // unwrap constant
        if self.passthrough_constant() && columns.iter().all(|v| v.column().is_const()) {
            let columns = columns
                .iter()
                .map(|v| {
                    let c = v.column();
                    let c: &ConstColumn = unsafe { Series::static_cast(c) };

                    ColumnWithField::new(c.inner().clone(), v.field().clone())
                })
                .collect::<Vec<_>>();

            let col = self.eval(&columns, 1, func_ctx)?;
            let col = if col.is_const() && col.len() == 1 {
                col.replicate(&[input_rows])
            } else if col.is_null() {
                NullColumn::new(input_rows).arc()
            } else {
                ConstColumn::new(col, input_rows).arc()
            };
            return Ok(col);
        }

        // nullable or null
        if self.passthrough_null {
            if columns
                .iter()
                .any(|v| v.data_type().data_type_id() == TypeID::Null)
            {
                return Ok(Arc::new(NullColumn::new(input_rows)));
            }

            if columns.iter().any(|v| v.data_type().is_nullable()) {
                let mut validity: Option<Bitmap> = None;
                let mut has_all_null = false;

                let columns = columns
                    .iter()
                    .map(|v| {
                        let (is_all_null, valid) = v.column().validity();
                        if is_all_null {
                            has_all_null = true;
                            let mut v = MutableBitmap::with_capacity(input_rows);
                            v.extend_constant(input_rows, false);
                            validity = Some(v.into());
                        } else if !has_all_null {
                            validity = combine_validities_2(validity.clone(), valid.cloned());
                        }

                        let ty = remove_nullable(v.data_type());
                        let f = v.field();
                        let col = Series::remove_nullable(v.column());
                        ColumnWithField::new(col, DataField::new(f.name(), ty))
                    })
                    .collect::<Vec<_>>();

                let col = self.eval(&columns, input_rows, func_ctx)?;

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
        }

        inner.eval(columns, input_rows, func_ctx)
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
use crate::scalars::FunctionContext;
