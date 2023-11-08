use std::cmp::Ordering;

use arrow2::array::ord::build_compare;
use arrow2::array::*;
use arrow2::datatypes::DataType;
use arrow2::error::Result;

#[test]
fn i32() -> Result<()> {
    let array = Int32Array::from_slice([1, 2]);

    let cmp = build_compare(&array, &array)?;

    assert_eq!(Ordering::Less, (cmp)(0, 1));
    Ok(())
}

#[test]
fn i32_i32() -> Result<()> {
    let array1 = Int32Array::from_slice([1]);
    let array2 = Int32Array::from_slice([2]);

    let cmp = build_compare(&array1, &array2)?;

    assert_eq!(Ordering::Less, (cmp)(0, 0));
    Ok(())
}

#[test]
fn f32() -> Result<()> {
    let array = &Float32Array::from_slice([1.0, 2.0]);

    let cmp = build_compare(array, array)?;

    assert_eq!(Ordering::Less, (cmp)(0, 1));
    Ok(())
}

#[test]
fn f64() -> Result<()> {
    let array = Float64Array::from_slice([1.0, 2.0]);

    let cmp = build_compare(&array, &array)?;

    assert_eq!(Ordering::Less, (cmp)(0, 1));
    Ok(())
}

#[test]
fn f64_nan() -> Result<()> {
    let array = Float64Array::from_slice([1.0, f64::NAN]);

    let cmp = build_compare(&array, &array)?;

    assert_eq!(Ordering::Less, (cmp)(0, 1));
    Ok(())
}

#[test]
fn f64_zeros() -> Result<()> {
    let array = Float64Array::from_slice([-0.0, 0.0]);

    let cmp = build_compare(&array, &array)?;

    // official IEEE 754 (2008 revision)
    assert_eq!(Ordering::Less, (cmp)(0, 1));
    assert_eq!(Ordering::Greater, (cmp)(1, 0));
    Ok(())
}

#[test]
fn decimal() -> Result<()> {
    let array = Int128Array::from_slice([1, 2]).to(DataType::Decimal(38, 0));

    let cmp = build_compare(&array, &array)?;

    assert_eq!(Ordering::Less, (cmp)(0, 1));
    assert_eq!(Ordering::Equal, (cmp)(1, 1));
    assert_eq!(Ordering::Greater, (cmp)(1, 0));

    Ok(())
}

#[test]
fn dict_utf8() -> Result<()> {
    let data = vec!["a", "b", "c", "a", "a", "c", "c"];

    let data = data.into_iter().map(Some);
    let mut array = MutableDictionaryArray::<i32, MutableUtf8Array<i32>>::new();
    array.try_extend(data)?;
    let array: DictionaryArray<i32> = array.into();

    let cmp = build_compare(&array, &array)?;

    assert_eq!(Ordering::Less, (cmp)(0, 1));
    assert_eq!(Ordering::Equal, (cmp)(3, 4));
    assert_eq!(Ordering::Greater, (cmp)(2, 3));
    Ok(())
}

#[test]
fn dict_i32() -> Result<()> {
    let data = vec![1, 2, 3, 1, 1, 3, 3];

    let data = data.into_iter().map(Some);

    let mut array = MutableDictionaryArray::<i32, MutablePrimitiveArray<i32>>::new();
    array.try_extend(data)?;
    let array = array.into_arc();

    let cmp = build_compare(array.as_ref(), array.as_ref())?;

    assert_eq!(Ordering::Less, (cmp)(0, 1));
    assert_eq!(Ordering::Equal, (cmp)(3, 4));
    assert_eq!(Ordering::Greater, (cmp)(2, 3));
    Ok(())
}
