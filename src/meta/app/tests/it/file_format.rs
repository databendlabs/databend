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

use databend_common_meta_app::principal::CsvFileFormatParams;
use databend_common_meta_app::principal::CsvQuoteStyle;
use databend_common_meta_app::principal::FileFormatOptionsReader;
use databend_common_meta_app::principal::FileFormatParams;
use databend_common_meta_app::principal::check_record_delimiter;

#[test]
fn test_csv_quote_styles() {
    assert_eq!(
        CsvFileFormatParams::default().quote_style,
        CsvQuoteStyle::QuoteNotNull
    );
    for (name, style) in [
        ("quote_not_null", CsvQuoteStyle::QuoteNotNull),
        ("quote_non_numeric", CsvQuoteStyle::QuoteNonNumeric),
        ("quote_minimal", CsvQuoteStyle::QuoteMinimal),
    ] {
        assert_eq!(name.parse::<CsvQuoteStyle>().unwrap(), style);
        assert_eq!(style.to_string(), name);
        let options = BTreeMap::from([
            ("type".to_string(), "csv".to_string()),
            ("quote_style".to_string(), name.to_uppercase()),
        ]);
        let params =
            FileFormatParams::try_from_reader(FileFormatOptionsReader::from_map(options), false)
                .unwrap();
        assert_eq!(
            CsvFileFormatParams::downcast_unchecked(&params).quote_style,
            style
        );
    }
    let error = "quote_invalid".parse::<CsvQuoteStyle>().unwrap_err();
    assert!(error.contains("QUOTE_NON_NUMERIC"));
}

#[test]
fn test_check_record_delimiter() {
    assert!(check_record_delimiter("|").is_ok());
    assert!(check_record_delimiter("\r\n").is_ok());
    assert!(check_record_delimiter("").is_err());
    assert!(check_record_delimiter("foo").is_err());
    assert!(check_record_delimiter("|\r").is_err());
}
