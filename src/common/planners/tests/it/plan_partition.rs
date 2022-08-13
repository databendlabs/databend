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

use std::any::Any;

use common_exception::Result;
use common_planners::PartInfo;

#[derive(serde::Serialize, serde::Deserialize, PartialEq)]
struct TestPartInfoA {
    field_a: usize,
    field_b: String,
}

#[typetag::serde(name = "TestA")]
impl PartInfo for TestPartInfoA {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn PartInfo>) -> bool {
        match info.as_any().downcast_ref::<TestPartInfoA>() {
            None => false,
            Some(other) => self == other,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq)]
struct TestPartInfoB {
    field_a: String,
    field_b: u64,
}

#[typetag::serde(name = "TestB")]
impl PartInfo for TestPartInfoB {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn PartInfo>) -> bool {
        match info.as_any().downcast_ref::<TestPartInfoB>() {
            None => false,
            Some(other) => self == other,
        }
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
    let info_a: Box<dyn PartInfo> =
        serde_json::from_str("{\"type\":\"TestA\",\"field_a\":123,\"field_b\":\"456\"}")?;
    let test_part_a = info_a.as_any().downcast_ref::<TestPartInfoA>().unwrap();
    assert_eq!(test_part_a.field_a, 123);
    assert_eq!(test_part_a.field_b, String::from("456"));

    let info_b = serde_json::from_str::<Box<dyn PartInfo>>(
        "{\"type\":\"TestB\",\"field_a\":\"123\",\"field_b\":456}",
    )?;
    let test_part_a = info_b.as_any().downcast_ref::<TestPartInfoB>().unwrap();
    assert_eq!(test_part_a.field_a, String::from("123"));
    assert_eq!(test_part_a.field_b, 456);

    Ok(())
}

#[test]
fn test_partial_equals_part_info() -> Result<()> {
    let info_a: Box<dyn PartInfo> = Box::new(TestPartInfoA {
        field_a: 123,
        field_b: String::from("456"),
    });

    let info_b: Box<dyn PartInfo> = Box::new(TestPartInfoB {
        field_a: String::from("123"),
        field_b: 456,
    });

    assert_ne!(&info_a, &info_b);
    assert_eq!(&info_a, &info_a);
    assert_eq!(&info_b, &info_b);
    Ok(())
}
