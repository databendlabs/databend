// Copyright 2021 Datafuse Labs
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

use databend_common_meta_app::principal::FileFormatParams;
use databend_common_meta_app::principal::LokiFileFormatParams;
use databend_common_meta_app::principal::NullAs;
use databend_common_meta_app::principal::StageFileFormatType;
use databend_common_proto_conv::FromToProto;
use databend_common_proto_conv::FromToProtoEnum;
use databend_common_protos::pb;
use fastrace::func_name;

use crate::common;

// Frozen v188 bytes: do not rewrite when metadata versions advance.
#[test]
fn test_decode_v188_loki_file_format_params() -> anyhow::Result<()> {
    let bytes = [
        10, 13, 70, 73, 69, 76, 68, 95, 68, 69, 70, 65, 85, 76, 84, 160, 6, 188, 1, 168, 6, 188, 1,
    ];
    let want = LokiFileFormatParams {
        missing_field_as: NullAs::FieldDefault,
    };
    common::test_load_old(func_name!(), &bytes, 188, want.clone())?;
    common::test_pb_from_to(func_name!(), want)?;
    Ok(())
}

#[test]
fn test_decode_v188_file_format_params_loki() -> anyhow::Result<()> {
    let bytes = [
        98, 23, 10, 13, 70, 73, 69, 76, 68, 95, 68, 69, 70, 65, 85, 76, 84, 160, 6, 188, 1, 168, 6,
        188, 1,
    ];
    let want = FileFormatParams::Loki(LokiFileFormatParams {
        missing_field_as: NullAs::FieldDefault,
    });
    common::test_load_old(func_name!(), &bytes, 0, want.clone())?;
    common::test_pb_from_to(func_name!(), want)?;
    Ok(())
}

#[test]
fn test_loki_format_defaults_and_validation() {
    assert_eq!(
        StageFileFormatType::Loki.to_pb_enum(),
        pb::StageFileFormatType::Loki
    );
    assert_eq!(
        StageFileFormatType::from_pb_enum(pb::StageFileFormatType::Loki).unwrap(),
        StageFileFormatType::Loki
    );
    for missing_field_as in [NullAs::Error, NullAs::Null, NullAs::FieldDefault] {
        let params = LokiFileFormatParams { missing_field_as };
        let pb = params.to_pb();
        assert_eq!(pb.min_reader_ver, 188);
        assert_eq!(LokiFileFormatParams::from_pb(pb).unwrap(), params);
    }
    let mut pb = LokiFileFormatParams::default().to_pb();
    pb.missing_field_as = None;
    assert_eq!(
        LokiFileFormatParams::from_pb(pb.clone()).unwrap(),
        LokiFileFormatParams::default()
    );
    pb.missing_field_as = Some("invalid".to_string());
    assert!(LokiFileFormatParams::from_pb(pb).is_err());
}
