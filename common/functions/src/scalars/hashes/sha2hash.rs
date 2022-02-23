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
use std::sync::Arc;

use common_datavalues::prelude::*;
use common_datavalues::StringType;
use common_datavalues::TypeID;
use common_exception::ErrorCode;
use common_exception::Result;
use sha2::Digest;

use crate::scalars::cast_column_field;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;
use crate::scalars::FunctionDescription;

#[derive(Clone)]
pub struct Sha2HashFunction {
    display_name: String,
}

impl Sha2HashFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(Sha2HashFunction {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(2))
    }
}

impl Function for Sha2HashFunction {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn return_type(
        &self,
        args: &[&common_datavalues::DataTypePtr],
    ) -> Result<common_datavalues::DataTypePtr> {
        if args[0].data_type_id() != TypeID::String {
            return Err(ErrorCode::IllegalDataType(format!(
                "Expected first arg as string type, but got {:?}",
                args[0]
            )));
        }

        if !args[1].data_type_id().is_numeric() {
            return Err(ErrorCode::IllegalDataType(format!(
                "Expected second arg as integer type, but got {:?}",
                args[1]
            )));
        }
        Ok(StringType::arc())
    }

    fn eval(
        &self,
        columns: &common_datavalues::ColumnsWithField,
        _input_rows: usize,
    ) -> Result<common_datavalues::ColumnRef> {
        let col_viewer = Vu8::try_create_viewer(columns[0].column())?;
        let const_col: Result<&ConstColumn> = Series::check_get(columns[1].column());

        if let Ok(col) = const_col {
            let l = col.get_u64(0)?;
            let col = match l {
                224 => {
                    let iter = col_viewer.iter().map(|i| {
                        let mut h = sha2::Sha224::new();
                        h.update(i);
                        format!("{:x}", h.finalize())
                    });

                    StringColumn::new_from_iter(iter)
                }
                256 | 0 => {
                    let iter = col_viewer.iter().map(|i| {
                        let mut h = sha2::Sha256::new();
                        h.update(i);
                        format!("{:x}", h.finalize())
                    });
                    StringColumn::new_from_iter(iter)
                }
                384 => {
                    let iter = col_viewer.iter().map(|i| {
                        let mut h = sha2::Sha384::new();
                        h.update(i);
                        format!("{:x}", h.finalize())
                    });
                    StringColumn::new_from_iter(iter)
                }
                512 => {
                    let iter = col_viewer.iter().map(|i| {
                        let mut h = sha2::Sha512::new();
                        h.update(i);
                        format!("{:x}", h.finalize())
                    });
                    StringColumn::new_from_iter(iter)
                }
                v => {
                    return Err(ErrorCode::BadArguments(format!(
                        "Expected [0, 224, 256, 384, 512] as sha2 encode options, but got {}",
                        v
                    )))
                }
            };

            Ok(Arc::new(col))
        } else {
            let l = cast_column_field(&columns[1], &UInt16Type::arc())?;
            let l_viewer = u16::try_create_viewer(&l)?;

            let mut col_builder = MutableStringColumn::with_capacity(l.len());
            for (i, l) in col_viewer.iter().zip(l_viewer.iter()) {
                match l {
                    224 => {
                        let mut h = sha2::Sha224::new();
                        h.update(i);
                        let res = format!("{:x}", h.finalize());
                        col_builder.append_value(res.as_bytes())
                    }
                    256 | 0 => {
                        let mut h = sha2::Sha256::new();
                        h.update(i);
                        let res = format!("{:x}", h.finalize());
                        col_builder.append_value(res.as_bytes())
                    }
                    384 => {
                        let mut h = sha2::Sha384::new();
                        h.update(i);
                        let res = format!("{:x}", h.finalize());
                        col_builder.append_value(res.as_bytes())
                    }
                    512 => {
                        let mut h = sha2::Sha512::new();
                        h.update(i);
                        let res = format!("{:x}", h.finalize());
                        col_builder.append_value(res.as_bytes())
                    }
                    v => {
                        return Err(ErrorCode::BadArguments(format!(
                            "Expected [0, 224, 256, 384, 512] as sha2 encode options, but got {}",
                            v
                        )))
                    }
                }
            }

            Ok(col_builder.to_column())
        }
    }
}

impl fmt::Display for Sha2HashFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}
