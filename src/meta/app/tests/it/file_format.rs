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

use std::collections::BTreeMap;
use std::str::FromStr;

use databend_common_ast::ast::ColumnMatchMode;
use databend_common_ast::ast::CopyIntoTableOptions;
use databend_common_meta_app::principal::FileFormatOptionsReader;
use databend_common_meta_app::principal::FileFormatParams;
use databend_common_meta_app::principal::LokiFileFormatParams;
use databend_common_meta_app::principal::NullAs;
use databend_common_meta_app::principal::StageFileCompression;
use databend_common_meta_app::principal::StageFileFormatType;
use databend_common_meta_app::principal::check_record_delimiter;

#[test]
fn test_loki_file_format_options() {
    assert_eq!(
        StageFileFormatType::from_str("lOkI").unwrap(),
        StageFileFormatType::Loki
    );
    assert_eq!(StageFileFormatType::Loki.to_string(), "LOKI");
    let params = FileFormatParams::default_by_type(StageFileFormatType::Loki).unwrap();
    assert_eq!(
        params,
        FileFormatParams::Loki(LokiFileFormatParams::default())
    );
    assert_eq!(params.compression(), StageFileCompression::None);
    assert_eq!(params.get_type(), StageFileFormatType::Loki);
    assert!(!params.need_field_default());
    for mode in [
        ColumnMatchMode::CaseSensitive,
        ColumnMatchMode::CaseInsensitive,
    ] {
        let mut options = CopyIntoTableOptions {
            column_match_mode: Some(mode),
            ..Default::default()
        };
        assert!(params.check_copy_options(&mut options).is_ok());
    }
    let mut options = CopyIntoTableOptions {
        column_match_mode: Some(ColumnMatchMode::Position),
        ..Default::default()
    };
    assert!(params.check_copy_options(&mut options).is_err());

    for (option, expected) in [
        ("ERROR", NullAs::Error),
        ("NULL", NullAs::Null),
        ("FIELD_DEFAULT", NullAs::FieldDefault),
    ] {
        let reader = FileFormatOptionsReader::from_map(BTreeMap::from([
            ("type".to_string(), "loki".to_string()),
            ("missing_field_as".to_string(), option.to_string()),
        ]));
        let params = FileFormatParams::try_from_reader(reader, false).unwrap();
        assert_eq!(
            params.need_field_default(),
            expected == NullAs::FieldDefault
        );
        assert_eq!(
            params,
            FileFormatParams::Loki(LokiFileFormatParams {
                missing_field_as: expected
            })
        );
    }
    for (key, value) in [
        ("compression", "gzip"),
        ("skip_header", "1"),
        ("missing_field_as", "invalid"),
    ] {
        let reader = FileFormatOptionsReader::from_map(BTreeMap::from([
            ("type".to_string(), "loki".to_string()),
            (key.to_string(), value.to_string()),
        ]));
        assert!(FileFormatParams::try_from_reader(reader, false).is_err());
    }
}

#[test]
fn test_check_record_delimiter() {
    assert!(check_record_delimiter("|").is_ok());
    assert!(check_record_delimiter("\r\n").is_ok());
    assert!(check_record_delimiter("").is_err());
    assert!(check_record_delimiter("foo").is_err());
    assert!(check_record_delimiter("|\r").is_err());
}
