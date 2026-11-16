// Copyright 2026 Datafuse Labs.
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

use databend_common_meta_app::principal::CsvFileFormatParams;
use databend_common_meta_app::principal::CsvQuoteStyle;
use databend_common_proto_conv::FromToProto;
use databend_common_proto_conv::MIN_READER_VER;
use fastrace::func_name;

use crate::common;

#[test]
fn test_decode_v193_csv_quote_non_numeric() -> anyhow::Result<()> {
    let csv_file_format_params_v193 = vec![
        8, 9, 26, 1, 44, 34, 1, 10, 42, 3, 78, 97, 78, 58, 1, 34, 66, 2, 92, 78, 82, 4, 78, 85, 76,
        76, 90, 3, 104, 101, 120, 106, 4, 69, 87, 75, 84, 122, 6, 83, 84, 82, 73, 78, 71, 130, 1,
        5, 85, 84, 70, 45, 56, 138, 1, 6, 115, 116, 114, 105, 99, 116, 154, 1, 17, 113, 117, 111,
        116, 101, 95, 110, 111, 110, 95, 110, 117, 109, 101, 114, 105, 99, 160, 6, 193, 1, 168, 6,
        193, 1,
    ];
    let want = CsvFileFormatParams {
        quote_style: CsvQuoteStyle::QuoteNonNumeric,
        ..Default::default()
    };
    assert_eq!(want.to_pb().min_reader_ver, 193);
    assert_eq!(
        CsvFileFormatParams::default().to_pb().min_reader_ver,
        MIN_READER_VER
    );
    common::test_load_old(
        func_name!(),
        &csv_file_format_params_v193,
        193,
        want.clone(),
    )?;
    common::test_pb_from_to(func_name!(), want)?;
    Ok(())
}
