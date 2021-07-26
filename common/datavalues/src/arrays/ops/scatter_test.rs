// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;

use crate::arrays::builders::*;
use crate::arrays::ops::scatter::ArrayScatter;
use crate::DFUInt16Array;

#[test]
fn test_scatter() -> Result<()> {

    // Create DFUint16Array
    let df_uint16_array = DFUInt16Array::new_from_iter(1u16..11u16);
    // Create the indice array 
    let indices = vec![1, 2, 3, 1, 3, 2, 0, 3, 1, 0];
    // The number of rows should be equal to the length of indices
    assert_eq!(df_uint16_array.len(), indices.len());

    let array_vec = df_uint16_array.scatter_unchecked(indices, 4)?;
    //let arrow_uint16_array: &PrimitiveArray<UInt16Type> = array_vec[0].as_ref();
    assert_eq!(&[7u16, 10], &array_vec[0].as_ref().values());
    assert_eq!(&[1u16, 4, 9], &array_vec[1].as_ref().values());
    assert_eq!(&[2u16, 6], &array_vec[2].as_ref().values());
    assert_eq!(&[3u16, 5, 8], &array_vec[3].as_ref().values());

    Ok(())
}