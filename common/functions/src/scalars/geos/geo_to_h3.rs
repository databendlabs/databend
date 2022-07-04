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

use std::fmt::Display;
use std::fmt::Formatter;

use common_datavalues::prelude::*;
use common_datavalues::with_match_primitive_type_id;
use common_datavalues::ColumnBuilder;
use common_datavalues::ColumnRef;
use common_datavalues::ColumnsWithField;
use common_datavalues::DataTypeImpl;
use common_datavalues::ScalarColumn;
use common_datavalues::Series;
use common_datavalues::TypeID;
use common_exception::ErrorCode;
use geo_types::Coordinate;
use h3ron::H3Cell;
use h3ron::Index;

use crate::scalars::Function;
use crate::scalars::FunctionContext;
use crate::scalars::FunctionDescription;
use crate::scalars::FunctionFeatures;

#[derive(Clone)]
pub struct GeoToH3Function {
    display_name: String,
}

impl GeoToH3Function {
    pub fn try_create(
        display_name: &str,
        args: &[&DataTypeImpl],
    ) -> common_exception::Result<Box<dyn Function>> {
        let mut arg = args[0];
        if arg.data_type_id() != TypeID::Float64 {
            return Err(ErrorCode::IllegalDataType(format!(
                "Invalid type {} of argument {} for function '{}'. Must be Float64",
                arg.data_type_id(),
                1,
                display_name,
            )));
        }

        arg = args[1];
        if arg.data_type_id() != TypeID::Float64 {
            return Err(ErrorCode::IllegalDataType(format!(
                "Invalid type {} of argument {} for function '{}'. Must be Float64",
                arg.data_type_id(),
                2,
                display_name,
            )));
        }

        arg = args[2];
        if arg.data_type_id() != TypeID::UInt8 {
            return Err(ErrorCode::IllegalDataType(format!(
                "Invalid type {} of argument {} for function '{}'. Must be Float64",
                arg.data_type_id(),
                1,
                display_name,
            )));
        }

        Ok(Box::new(Self {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(3))
    }
}

impl Function for GeoToH3Function {
    fn name(&self) -> &str {
        &self.display_name
    }

    fn return_type(&self) -> DataTypeImpl {
        UInt64Type::new_impl()
    }

    fn eval(
        &self,
        _func_ctx: FunctionContext,
        columns: &ColumnsWithField,
        input_rows: usize,
    ) -> common_exception::Result<ColumnRef> {
        with_match_primitive_type_id!(columns[0].data_type().data_type_id(), |$S| {
            with_match_primitive_type_id!(columns[1].data_type().data_type_id(), |$T| {
                with_match_primitive_type_id!(columns[2].data_type().data_type_id(), |$R| {
                    let data_lon = Series::check_get_scalar::<f64>(columns[0].column())?;
                    let data_lat = Series::check_get_scalar::<f64>(columns[1].column())?;
                    let data_res = Series::check_get_scalar::<u8>(columns[2].column())?;

                    let mut builder: ColumnBuilder<u64> = ColumnBuilder::with_capacity(input_rows);

                    for i in 0..input_rows {
                        let lon = data_lon.get_data(i);
                        let lat = data_lat.get_data(i);
                        let res = data_res.get_data(i);

                        // x must be Longitude and y must be Latitude
                        // `h3ron` will transform `Coordinate{x, y}` to `GeoCoord{lat:y, lon:x}` internally.
                        let coord = Coordinate { x: lon, y: lat };
                        let h3_cell = H3Cell::from_coordinate_unchecked(&coord, res);
                        builder.append(h3_cell.h3index());
                    }

                    Ok(builder.build(input_rows))
                },{
                    unreachable!()
                })
            },{
                unreachable!()
            })
        },{
            unreachable!()
        })
    }
}

impl Display for GeoToH3Function {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.display_name.to_uppercase())
    }
}
