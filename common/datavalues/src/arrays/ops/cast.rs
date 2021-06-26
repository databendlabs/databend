// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_arrow::arrow::compute::cast;
use common_exception::ErrorCode;
use common_exception::Result;
use num::NumCast;

use crate::arrays::DataArray;
use crate::data_df_type::*;
use crate::series::IntoSeries;
use crate::series::Series;
use crate::DFDataType;
use crate::DFNumericType;
use crate::DataType;

/// Cast `DataArray<T>` to `DataArray<N>`
pub trait ArrayCast {
    /// Cast `DataArray<T>` to `DataArray<N>`
    fn cast<N>(&self) -> Result<DataArray<N>>
    where N: DFDataType;

    fn cast_with_type(&self, _data_type: &DataType) -> Result<Series>;
}

fn cast_ca<N, T>(ca: &DataArray<T>) -> Result<DataArray<N>>
where
    N: DFDataType,
    T: DFDataType,
{
    if N::data_type() == T::data_type() {
        // convince the compiler that N and T are the same type
        return unsafe {
            let ca = std::mem::transmute(ca.clone());
            Ok(ca)
        };
    }

    let ca = cast(&ca.array, &N::data_type().to_arrow())?;
    Ok(ca.into())
}

macro_rules! cast_with_type {
    ($self:expr, $data_type:expr) => {{
        use crate::data_type::DataType::*;
        match $data_type {
            Boolean => ArrayCast::cast::<BooleanType>($self).map(|ca| ca.into_series()),
            Utf8 => ArrayCast::cast::<Utf8Type>($self).map(|ca| ca.into_series()),
            UInt8 => ArrayCast::cast::<UInt8Type>($self).map(|ca| ca.into_series()),
            UInt16 => ArrayCast::cast::<UInt16Type>($self).map(|ca| ca.into_series()),
            UInt32 => ArrayCast::cast::<UInt32Type>($self).map(|ca| ca.into_series()),
            UInt64 => ArrayCast::cast::<UInt64Type>($self).map(|ca| ca.into_series()),
            Int8 => ArrayCast::cast::<Int8Type>($self).map(|ca| ca.into_series()),
            Int16 => ArrayCast::cast::<Int16Type>($self).map(|ca| ca.into_series()),
            Int32 => ArrayCast::cast::<Int32Type>($self).map(|ca| ca.into_series()),
            Int64 => ArrayCast::cast::<Int64Type>($self).map(|ca| ca.into_series()),
            Float32 => ArrayCast::cast::<Float32Type>($self).map(|ca| ca.into_series()),
            Float64 => ArrayCast::cast::<Float64Type>($self).map(|ca| ca.into_series()),
            Date32 => ArrayCast::cast::<Date32Type>($self).map(|ca| ca.into_series()),
            Date64 => ArrayCast::cast::<Date64Type>($self).map(|ca| ca.into_series()),

            List(_) => ArrayCast::cast::<ListType>($self).map(|ca| ca.into_series()),
            dt => Err(ErrorCode::IllegalDataType(format!(
                "Arrow datatype {:?} not supported by Datafuse",
                dt
            ))),
        }
    }};
}

impl<T> ArrayCast for DataArray<T>
where
    T: DFNumericType,
    T::Native: NumCast,
{
    fn cast<N>(&self) -> Result<DataArray<N>>
    where N: DFDataType {
        cast_ca(&self)
    }

    fn cast_with_type(&self, data_type: &DataType) -> Result<Series> {
        cast_with_type!(self, data_type)
    }
}

impl ArrayCast for DataArray<Utf8Type> {
    fn cast<N>(&self) -> Result<DataArray<N>>
    where N: DFDataType {
        cast_ca(self)
    }

    fn cast_with_type(&self, data_type: &DataType) -> Result<Series> {
        cast_with_type!(self, data_type)
    }
}

impl ArrayCast for DFBooleanArray {
    fn cast<N>(&self) -> Result<DataArray<N>>
    where N: DFDataType {
        cast_ca(self)
    }
    fn cast_with_type(&self, data_type: &DataType) -> Result<Series> {
        cast_with_type!(self, data_type)
    }
}

impl ArrayCast for DFListArray {
    fn cast<N>(&self) -> Result<DataArray<N>>
    where N: DFDataType {
        todo!()
    }

    fn cast_with_type(&self, _data_type: &DataType) -> Result<Series> {
        todo!()
    }
}

impl ArrayCast for DFNullArray {
    fn cast<N>(&self) -> Result<DataArray<N>>
    where N: DFDataType {
        todo!()
    }

    fn cast_with_type(&self, _data_type: &DataType) -> Result<Series> {
        todo!()
    }
}

impl ArrayCast for DFStructArray {
    fn cast<N>(&self) -> Result<DataArray<N>>
    where N: DFDataType {
        todo!()
    }

    fn cast_with_type(&self, _data_type: &DataType) -> Result<Series> {
        todo!()
    }
}

impl ArrayCast for DFBinaryArray {
    fn cast<N>(&self) -> Result<DataArray<N>>
    where N: DFDataType {
        todo!()
    }

    fn cast_with_type(&self, _data_type: &DataType) -> Result<Series> {
        todo!()
    }
}
