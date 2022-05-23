use common_exception::Result;
use common_io::prelude::FormatSettings;

use crate::ColumnRef;
use crate::DataType;
use crate::TypeSerializer;

#[allow(dead_code)]
pub fn write_vec(col: &ColumnRef) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1000 * 1000);

    let s = col.data_type().create_serializer();
    let v = s
        .serialize_column(&col, &FormatSettings::default())
        .unwrap();
    for field in v {
        buf.extend_from_slice(&field.as_bytes());
    }
    buf
}

pub fn write_by_row(col: &ColumnRef) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1000 * 1000);
    let rows = col.len();
    let s = col.data_type().create_serializer();
    let f = &FormatSettings::default();
    for row in 0..rows {
        s.write_csv_field(col, row, &mut buf, f).unwrap();
    }
    buf
}

pub fn write_iterator(col: &ColumnRef) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1000 * 1000);

    let s = col.data_type().create_serializer();
    let mut stream = s.serialize_csv(&col, &FormatSettings::default()).unwrap();
    while let Some(field) = stream.next() {
        buf.extend_from_slice(field);
    }
    buf
}

pub fn write_embedded(col: &ColumnRef) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1000 * 1000);
    let s = col.data_type().create_serializer();
    let rows = col.len();
    let ss = s.get_csv_serializer(col).unwrap();
    let f = &FormatSettings::default();
    for row in 0..rows {
        ss.write_csv_field(row, &mut buf, f).unwrap();
    }
    buf
}

#[test]
fn test_writers() -> Result<()> {
    use crate::Series;
    use crate::SeriesFrom;
    let col = Series::from_data(vec![12u8, 23u8, 34u8]);
    let exp = [49, 50, 50, 51, 51, 52];
    assert_eq!(write_iterator(&col), exp);
    assert_eq!(write_by_row(&col), exp);
    assert_eq!(write_embedded(&col), exp);

    let col = Series::from_data(vec![Some(12u8), None, Some(34u8)]);
    let exp = [49, 50, 0, 51, 52];
    assert_eq!(write_iterator(&col), exp);
    assert_eq!(write_by_row(&col), exp);
    assert_eq!(write_embedded(&col), exp);

    let col = Series::from_data(vec!["12", "34"]);
    let exp = "1234".to_string().as_bytes().to_vec();
    assert_eq!(write_iterator(&col), exp);
    assert_eq!(write_by_row(&col), exp);
    assert_eq!(write_embedded(&col), exp);

    let col = Series::from_data(vec![Some(12u8), None, Some(34u8)]);
    let exp = [49, 50, 0, 51, 52];
    assert_eq!(write_iterator(&col), exp);
    assert_eq!(write_by_row(&col), exp);
    assert_eq!(write_embedded(&col), exp);
    Ok(())
}

#[test]
fn test_debug() -> Result<()> {
    use crate::Series;
    use crate::SeriesFrom;
    use crate::TypeSerializer;
    // let col = Series::from_data(vec![true, false, true]);
    // let col = Series::from_data(vec!["a", "a", "bc"]);
    // let col = Series::from_data(vec![12, 23, 34]);
    let col = Series::from_data(vec![12u8, 23u8, 34u8]);

    println!("{:?}", col);
    let s = col.data_type().create_serializer();
    let mut stream = s.serialize_csv(&col, &FormatSettings::default())?;
    println!("{:?}", stream.next());
    println!("{:?}", stream.next());
    println!("{:?}", stream.next());
    println!("{:?}", stream.next());

    let col = Series::from_data(vec![Some(12), None, Some(34)]);
    println!("{:?}", col);
    let s = col.data_type().create_serializer();
    let mut stream = s.serialize_csv(&col, &FormatSettings::default())?;
    println!("{:?}", stream.next());
    println!("{:?}", stream.next());
    println!("{:?}", stream.next());
    println!("{:?}", stream.next());
    Ok(())
}
