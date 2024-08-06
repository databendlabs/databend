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

use std::vec;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_exception::Result;
use databend_common_expression::arrow::deserialize_column;
use databend_common_expression::arrow::serialize_column;
use databend_common_expression::types::DataType;
use databend_common_expression::types::StringType;
use databend_common_expression::Column;
use databend_common_expression::FromData;
use databend_common_expression::RemoteExpr;
use databend_common_expression::Scalar;
use databend_common_io::prelude::borsh_deserialize_from_slice;
use databend_common_io::prelude::borsh_serialize_into_buf;

#[test]
fn test_serde_column() -> Result<()> {
    #[derive(serde::Serialize, serde::Deserialize, Eq, PartialEq, Debug)]
    struct Plan {
        column: Column,
    }

    let column = StringType::from_data(vec!["SM CASE", "a", "b", "e", "f", "g"]);
    let plan = Plan { column };

    {
        let json = serde_json::to_vec(&plan).unwrap();
        let new_plan = serde_json::from_slice::<Plan>(&json).unwrap();
        assert_eq!(plan, new_plan);
    }

    {
        let vs = serde_json::to_vec(&plan).unwrap();
        let new_plan: Plan = serde_json::from_slice(&vs).unwrap();
        assert_eq!(plan, new_plan);
    }
    Ok(())
}

#[test]
fn test_serde_expr() -> Result<()> {
    let column = StringType::from_data(vec!["SM CASE", "a", "b", "e", "f", "g"]);
    let expr = RemoteExpr::<usize>::Constant {
        span: None,
        scalar: Scalar::Array(column),
        data_type: DataType::Array(Box::new(DataType::String)),
    };

    let json = serde_json::to_vec(&expr).unwrap();
    let new_expr = serde_json::from_slice::<RemoteExpr>(&json).unwrap();

    assert!(expr == new_expr);
    Ok(())
}

#[test]
fn test_serde_bin_column() -> Result<()> {
    let columns = vec![
        StringType::from_data(vec!["SM CASE", "a", "b", "e", "f", "g"]),
        StringType::from_data(vec!["SM CASE", "axx", "bxx", "xxe", "eef", "fg"]),
    ];

    for col in columns {
        let data = serialize_column(&col);
        let t = deserialize_column(&data).unwrap();
        assert_eq!(col, t);
    }
    Ok(())
}

#[test]
fn test_borsh_serde_column() -> Result<()> {
    #[derive(BorshSerialize, BorshDeserialize, Eq, PartialEq, Debug)]
    struct Plan {
        column: Column,
    }

    let column = StringType::from_data(vec!["SM CASE", "a", "b", "e", "f", "g"]);
    let plan = Plan { column };

    {
        let json = borsh::to_vec(&plan).unwrap();
        let new_plan = borsh::from_slice::<Plan>(&json).unwrap();
        assert!(plan == new_plan);
    }

    {
        let mut vs = vec![];
        borsh_serialize_into_buf(&mut vs, &plan).unwrap();
        let vs = vs.as_slice();
        let new_plan: Plan = borsh_deserialize_from_slice(vs).unwrap();
        assert!(plan == new_plan);
    }
    Ok(())
}
