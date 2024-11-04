// Copyright 2023 Datafuse Labs.
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

use databend_common_meta_app as mt;
use databend_common_meta_app::principal::CsvFileFormatParams;
use databend_common_meta_app::principal::NullAs;
use databend_common_meta_app::principal::StageFileCompression;
use databend_common_meta_app::principal::TsvFileFormatParams;
use fastrace::func_name;

use crate::common;
use crate::v032_file_format_params::mt::principal::JsonFileFormatParams;
use crate::v032_file_format_params::mt::principal::NdJsonFileFormatParams;
use crate::v032_file_format_params::mt::principal::ParquetFileFormatParams;
use crate::v032_file_format_params::mt::principal::XmlFileFormatParams;

// These bytes are built when a new version in introduced,
// and are kept for backward compatibility test.
//
// *************************************************************
// * These messages should never be updated,                   *
// * only be added when a new version is added,                *
// * or be removed when an old version is no longer supported. *
// *************************************************************
//
#[test]
fn test_decode_v32_csv_file_format_params() -> anyhow::Result<()> {
    let file_format_params_v32 = vec![
        18, 29, 8, 1, 16, 1, 26, 2, 102, 100, 34, 2, 114, 100, 42, 3, 110, 97, 110, 50, 1, 92, 58,
        1, 39, 160, 6, 32, 168, 6, 24,
    ];
    let want = || {
        mt::principal::FileFormatParams::Csv(CsvFileFormatParams {
            compression: StageFileCompression::Gzip,
            headers: 1,
            output_header: false,
            field_delimiter: "fd".to_string(),
            record_delimiter: "rd".to_string(),
            null_display: "\\N".to_string(),
            nan_display: "nan".to_string(),
            escape: "\\".to_string(),
            quote: "\'".to_string(),
            error_on_column_count_mismatch: true,
            empty_field_as: Default::default(),
            binary_format: Default::default(),
            geometry_format: Default::default(),
        })
    };
    common::test_load_old(func_name!(), file_format_params_v32.as_slice(), 0, want())?;
    common::test_pb_from_to(func_name!(), want())?;
    Ok(())
}

#[test]
fn test_decode_v32_tsv_file_format_params() -> anyhow::Result<()> {
    let file_format_params_v32 = vec![
        26, 29, 8, 1, 16, 1, 26, 2, 102, 100, 34, 2, 114, 100, 42, 3, 110, 97, 110, 50, 1, 92, 58,
        1, 39, 160, 6, 32, 168, 6, 24,
    ];

    let want = || {
        mt::principal::FileFormatParams::Tsv(TsvFileFormatParams {
            compression: StageFileCompression::Gzip,
            headers: 1,
            field_delimiter: "fd".to_string(),
            record_delimiter: "rd".to_string(),
            nan_display: "nan".to_string(),
            escape: "\\".to_string(),
            quote: "\'".to_string(),
        })
    };
    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), file_format_params_v32.as_slice(), 0, want())?;
    Ok(())
}

#[test]
fn test_decode_v32_ndjson_file_format_params() -> anyhow::Result<()> {
    let file_format_params_v32 = vec![42, 8, 8, 1, 160, 6, 32, 168, 6, 24];

    let want = || {
        mt::principal::FileFormatParams::NdJson(NdJsonFileFormatParams {
            compression: StageFileCompression::Gzip,
            missing_field_as: NullAs::Error,
            null_field_as: NullAs::Null,
            null_if: vec![],
        })
    };
    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), file_format_params_v32.as_slice(), 0, want())?;
    Ok(())
}

#[test]
fn test_decode_v32_json_file_format_params() -> anyhow::Result<()> {
    let file_format_params_v32 = vec![34, 8, 8, 1, 160, 6, 32, 168, 6, 24];
    let want = || {
        mt::principal::FileFormatParams::Json(JsonFileFormatParams {
            compression: StageFileCompression::Gzip,
        })
    };
    common::test_load_old(func_name!(), file_format_params_v32.as_slice(), 0, want())?;
    common::test_pb_from_to(func_name!(), want())?;
    Ok(())
}

#[test]
fn test_decode_v32_xml_file_format_params() -> anyhow::Result<()> {
    let file_format_params_v32 = vec![
        50, 17, 8, 1, 18, 7, 114, 111, 119, 95, 116, 97, 103, 160, 6, 32, 168, 6, 24,
    ];

    let want = || {
        mt::principal::FileFormatParams::Xml(XmlFileFormatParams {
            compression: StageFileCompression::Gzip,
            row_tag: "row_tag".to_string(),
        })
    };
    common::test_load_old(func_name!(), file_format_params_v32.as_slice(), 0, want())?;
    common::test_pb_from_to(func_name!(), want())?;
    Ok(())
}

#[test]
fn test_decode_v32_parquet_file_format_params() -> anyhow::Result<()> {
    let file_format_params_v32 = vec![10, 6, 160, 6, 32, 168, 6, 24];

    let want = || {
        mt::principal::FileFormatParams::Parquet(ParquetFileFormatParams {
            missing_field_as: Default::default(),
            null_if: vec![],
        })
    };
    common::test_load_old(func_name!(), file_format_params_v32.as_slice(), 0, want())?;
    common::test_pb_from_to(func_name!(), want())?;
    Ok(())
}
