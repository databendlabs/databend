// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, FixedSizeListArray};
use arrow_cast::CastOptions;
use arrow_schema::{ArrowError, DataType};

/// Customized [`arrow_cast::cast_with_options`] that handles cases not supported upstream yet.
pub fn cast_with_options(
    array: &dyn Array,
    to_type: &DataType,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    use DataType::*;
    match (array.data_type(), to_type) {
        (FixedSizeList(_, size_from), FixedSizeList(to_field, size_to)) if size_from == size_to => {
            let array = array.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
            let values = cast_with_options(array.values(), to_field.data_type(), cast_options)?;
            Ok(Arc::new(FixedSizeListArray::try_new(
                to_field.clone(),
                *size_from,
                values,
                array.nulls().cloned(),
            )?))
        }
        _ => arrow_cast::cast_with_options(array, to_type, cast_options),
    }
}
