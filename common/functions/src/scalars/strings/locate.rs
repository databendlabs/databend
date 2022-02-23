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

use common_datavalues::prelude::*;
use common_exception::Result;

use crate::scalars::assert_numeric;
use crate::scalars::assert_string;
use crate::scalars::default_column_cast;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;
use crate::scalars::FunctionDescription;

const FUNC_LOCATE: u8 = 1;
const FUNC_POSITION: u8 = 2;
const FUNC_INSTR: u8 = 3;

pub type LocateFunction = LocatingFunction<FUNC_LOCATE>;
pub type PositionFunction = LocatingFunction<FUNC_POSITION>;
pub type InstrFunction = LocatingFunction<FUNC_INSTR>;

#[derive(Clone)]
pub struct LocatingFunction<const T: u8> {
    display_name: String,
}

impl<const T: u8> LocatingFunction<T> {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(LocatingFunction::<T> {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        let mut feature = FunctionFeatures::default().deterministic();
        feature = if T == FUNC_LOCATE {
            feature.variadic_arguments(2, 3)
        } else {
            feature.num_arguments(2)
        };

        FunctionDescription::creator(Box::new(Self::try_create)).features(feature)
    }
}

impl<const T: u8> Function for LocatingFunction<T> {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn return_type(&self, args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        assert_string(args[0])?;
        assert_string(args[1])?;
        if args.len() > 2 {
            assert_numeric(args[2])?;
        }
        Ok(u64::to_data_type())
    }

    fn eval(&self, columns: &ColumnsWithField, input_rows: usize) -> Result<ColumnRef> {
        let (ss_column, s_column) = if T == FUNC_INSTR {
            (columns[1].column(), columns[0].column())
        } else {
            (columns[0].column(), columns[1].column())
        };

        // TODO, improve the performance using matching macros.
        let p_column = if T == FUNC_LOCATE && columns.len() == 3 {
            default_column_cast(columns[2].column(), &u64::to_data_type())?
        } else {
            ConstColumn::new(Series::from_data(vec![1u64]), input_rows).arc()
        };

        let ss_column = Vu8::try_create_viewer(ss_column)?;
        let s_column = Vu8::try_create_viewer(s_column)?;
        let p_column = u64::try_create_viewer(&p_column)?;

        let iter = ss_column
            .iter()
            .zip(s_column.iter())
            .zip(p_column.iter())
            .map(|((ss, s), p)| find_at(s, ss, p));
        Ok(UInt64Column::from_owned_iterator(iter).arc())
    }
}

impl<const T: u8> fmt::Display for LocatingFunction<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

#[inline]
fn find_at(str: &[u8], substr: &[u8], pos: u64) -> u64 {
    let pos = pos as usize;
    if pos == 0 {
        return 0_u64;
    }
    let p = pos - 1;
    if p + substr.len() <= str.len() {
        str[p..]
            .windows(substr.len())
            .position(|w| w == substr)
            .map(|i| i + 1 + p)
            .unwrap_or(0) as u64
    } else {
        0_u64
    }
}
