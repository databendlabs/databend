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

use std::fmt;
use std::hash::Hasher;
use std::sync::Arc;

use common_datavalues::prelude::*;
use common_datavalues::with_match_scalar_types_error;
use common_datavalues::TypeID;
use common_exception::ErrorCode;
use common_exception::Result;
use naive_cityhash::cityhash64_with_seed;

use super::hash_base::DFHash;
use crate::scalars::cast_column_field;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;
use crate::scalars::FunctionDescription;

// This is not a correct implementation of stateful hasher. But just a thin wrapper of stateless hash for leveraging DFHash trait.
// It is good enough for column hashing because we don't really need stream hashing.
// Keep the struct within this file and don't expose outside.
struct CityHasher64 {
    seed: u64,
    value: u64,
}

impl CityHasher64 {
    fn with_seed(s: u64) -> Self {
        Self { seed: s, value: 0 }
    }
}

impl Hasher for CityHasher64 {
    fn finish(&self) -> u64 {
        self.value
    }

    fn write(&mut self, bytes: &[u8]) {
        self.value = cityhash64_with_seed(bytes, self.seed);
    }
}

#[derive(Clone)]
pub struct City64WithSeedFunction {
    display_name: String,
}

// CityHash64WithSeed(value, seed)
impl City64WithSeedFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(City64WithSeedFunction {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(2))
    }
}

impl Function for City64WithSeedFunction {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn get_monotonicity(
        &self,
        _args: &[crate::scalars::Monotonicity],
    ) -> Result<crate::scalars::Monotonicity> {
        Ok(crate::scalars::Monotonicity::default())
    }

    fn return_type(
        &self,
        args: &[&common_datavalues::DataTypePtr],
    ) -> Result<common_datavalues::DataTypePtr> {
        if !matches!(
            args[0].data_type_id(),
            TypeID::UInt8
                | TypeID::UInt16
                | TypeID::UInt32
                | TypeID::UInt64
                | TypeID::Int8
                | TypeID::Int16
                | TypeID::Int32
                | TypeID::Int64
                | TypeID::Float32
                | TypeID::Float64
                | TypeID::Date16
                | TypeID::Date32
                | TypeID::DateTime32
                | TypeID::DateTime64
                | TypeID::Interval
                | TypeID::String
        ) {
            return Err(ErrorCode::IllegalDataType(format!(
                "Unsupported data type: {:?}",
                args[0]
            )));
        }

        if !args[1].data_type_id().is_numeric() {
            return Err(ErrorCode::IllegalDataType(format!(
                "Unsupported data type of hash seed: {:?}",
                args[1]
            )));
        }
        Ok(UInt64Type::arc())
    }

    fn eval(
        &self,
        columns: &common_datavalues::ColumnsWithField,
        _input_rows: usize,
    ) -> Result<common_datavalues::ColumnRef> {
        let column = columns[0].column();
        let physical_data_type = columns[0].data_type().data_type_id().to_physical_type();

        let const_col: Result<&ConstColumn> = Series::check_get(columns[1].column());

        if let Ok(col) = const_col {
            let seed = col.get_u64(0)?;
            let mut hasher = CityHasher64::with_seed(seed);

            let result_col = with_match_scalar_types_error!(physical_data_type, |$S| {
                let data_col: &<$S as Scalar>::ColumnType = Series::check_get(column)?;
                let iter = data_col.iter().map(|v| {
                    v.hash(&mut hasher);
                    hasher.finish()
                });
                UInt64Column::from_iterator(iter)
            });
            Ok(Arc::new(result_col))
        } else {
            let seed_col = cast_column_field(&columns[1], &UInt64Type::arc())?;
            let seed_viewer = u64::try_create_viewer(&seed_col)?;

            let result_col = with_match_scalar_types_error!(physical_data_type, |$S| {
                let data_col: &<$S as Scalar>::ColumnType = Series::check_get(column)?;
                let iter = data_col.iter().zip(seed_viewer.iter()).map(|(v, seed)| {
                    let mut hasher = CityHasher64::with_seed(seed);
                    v.hash(&mut hasher);
                    hasher.finish()
                });
                UInt64Column::from_iterator(iter)
            });
            Ok(Arc::new(result_col))
        }
    }
}

impl fmt::Display for City64WithSeedFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}
