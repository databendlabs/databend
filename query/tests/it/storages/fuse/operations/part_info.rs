//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use common_exception::ErrorCode;
use common_exception::Result;
use databend_query::storages::fuse::operations::PartInfo;

#[test]
fn test_part_info_encode_decode() -> Result<()> {
    // Normal
    let info = PartInfo::new("test_loc", 1);
    let encode_str = info.encode();
    let decoded = PartInfo::decode(&encode_str)?;
    assert_eq!(info, decoded);

    // Malformed
    {
        let too_much_parts = "xxx-1-xxx";
        let decoded = PartInfo::decode(too_much_parts);
        assert!(decoded.is_err());
        assert_eq!(decoded.unwrap_err().code(), ErrorCode::logical_error_code());
    }

    {
        let no_enough_parts = vec!["xxx", "-xxx", "xxx-"];
        no_enough_parts.iter().for_each(|val| {
            let decoded = PartInfo::decode(val);
            assert!(decoded.is_err());
            assert_eq!(decoded.unwrap_err().code(), ErrorCode::logical_error_code());
        });
    }

    {
        let invalid_number = "xxx-xxx";
        let decoded = PartInfo::decode(invalid_number);
        assert!(decoded.is_err());
        assert_eq!(decoded.unwrap_err().code(), ErrorCode::logical_error_code());
    }
    Ok(())
}
