use std::any::Any;
use common_exception::Result;
use common_planners::{PartInfo};

#[derive(serde::Serialize, serde::Deserialize)]
struct TestPartInfoA {
    field_a: usize,
    field_b: String,
}

#[typetag::serde(name = "TestA")]
impl PartInfo for TestPartInfoA {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct TestPartInfoB {
    field_a: String,
    field_b: u64,
}

#[typetag::serde(name = "TestB")]
impl PartInfo for TestPartInfoB {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[test]
fn test_serialize_part_info() -> Result<()> {
    let info_a: Box<dyn PartInfo> = Box::new(TestPartInfoA {
        field_a: 123,
        field_b: String::from("456"),
    });

    let info_b: Box<dyn PartInfo> = Box::new(TestPartInfoB {
        field_a: String::from("123"),
        field_b: 456,
    });


    assert_eq!(
        serde_json::to_string(&info_a)?,
        "{\"type\":\"TestA\",\"field_a\":123,\"field_b\":\"456\"}"
    );

    assert_eq!(
        serde_json::to_string(&info_b)?,
        "{\"type\":\"TestB\",\"field_a\":\"123\",\"field_b\":456}"
    );

    Ok(())
}

#[test]
fn test_deserialize_part_info() -> Result<()> {
    let info_a: Box<dyn PartInfo> = serde_json::from_str("{\"type\":\"TestA\",\"field_a\":123,\"field_b\":\"456\"}")?;
    let test_part_a = info_a.as_any().downcast_ref::<TestPartInfoA>().unwrap();
    assert_eq!(test_part_a.field_a, 123);
    assert_eq!(test_part_a.field_b, String::from("456"));

    let info_b = serde_json::from_str::<Box<dyn PartInfo>>("{\"type\":\"TestB\",\"field_a\":\"123\",\"field_b\":456}")?;
    let test_part_a = info_b.as_any().downcast_ref::<TestPartInfoB>().unwrap();
    assert_eq!(test_part_a.field_a, String::from("123"));
    assert_eq!(test_part_a.field_b, 456);

    Ok(())
}