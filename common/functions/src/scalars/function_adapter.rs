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

#[derive(Clone)]
pub struct FunctionAdapter {
    inner: Box<dyn Function>,
}

impl FunctionAdapter {
    pub fn create(inner: Box<dyn Function>) -> Box<dyn Function> {
        Box::new(Self { inner })
    }
}
impl Function for FunctionAdapter {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn return_type(&self, args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        if self.passthrough_null() {
            // one is null, result is null
            if args.iter().any(|v| v.data_type_id() == TypeID::Null) {
                return Ok(NullType::arc());
            }
            let has_nullable = args.iter().any(|v| v.is_nullable());
            let types = args.iter().map(|v| remove_nullable(v)).collect::<Vec<_>>();
            let types = types.iter().collect::<Vec<_>>();
            let typ = self.inner.return_type(&types)?;

            if has_nullable {
                Ok(wrap_nullable(&typ))
            } else {
                Ok(typ)
            }
        } else {
            self.inner.return_type(args)
        }
    }

    fn eval(&self, columns: &ColumnsWithField, input_rows: usize) -> Result<ColumnRef> {
        if columns.is_empty() {
            return self.inner.eval(columns, input_rows);
        }

        // unwrap nullable
        if self.passthrough_null() {
            // one is null, result is null
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

                let col = self.eval(&columns, input_rows)?;

                // The'try' series functions always return Null when they failed the try.
                // For example, try_inet_aton("helloworld") will return Null because it failed to parse "helloworld" to a valid IP address.
                // The same thing may happen on other 'try' functions. So we need to merge the validity.
                if col.is_nullable() {
                    let (_, bitmap) = col.validity();
                    validity = match validity {
                        Some(v) => combine_validities(bitmap, Some(&v)),
                        None => combine_validities(bitmap, None),
                    };
                }

                let validity = match validity {
                    Some(v) => v,
                    None => {
                        let mut v = MutableBitmap::with_capacity(input_rows);
                        v.extend_constant(input_rows, true);
                        v.into()
                    }
                };

                let col = if col.is_nullable() {
                    let nullable_column: &NullableColumn = unsafe { Series::static_cast(&col) };
                    NullableColumn::new(nullable_column.inner().clone(), validity)
                } else {
                    NullableColumn::new(col, validity)
                };
                return Ok(Arc::new(col));
            }
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

            let col = self.eval(&columns, 1)?;
            let col = if col.is_const() && col.len() == 1 {
                col.replicate(&[input_rows])
            } else if col.is_null() {
                NullColumn::new(input_rows).arc()
            } else {
                ConstColumn::new(col, input_rows).arc()
            };

            return Ok(col);
        }

        self.inner.eval(columns, input_rows)
    }

    fn get_monotonicity(&self, args: &[Monotonicity]) -> Result<Monotonicity> {
        self.inner.get_monotonicity(args)
    }

    fn passthrough_null(&self) -> bool {
        self.inner.passthrough_null()
    }

    fn passthrough_constant(&self) -> bool {
        self.inner.passthrough_constant()
    }
}

impl std::fmt::Display for FunctionAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.inner)
    }
}
