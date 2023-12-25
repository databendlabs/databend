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

use databend_common_formats::field_encoder::helpers::write_json_string;
use databend_common_formats::field_encoder::helpers::write_tsv_escaped_string;
use databend_common_formats::field_encoder::write_csv_string;

#[test]
fn test_escape() {
    {
        let mut buf = vec![];
        write_tsv_escaped_string(b"\0\n\r\t\\\'", &mut buf, b'\t');
        assert_eq!(&buf, b"\\0\\n\\r\\t\\\\\'")
    }

    {
        let mut buf = vec![];
        write_tsv_escaped_string(b"\n123\n456\n", &mut buf, b'\t');
        assert_eq!(&buf, b"\\n123\\n456\\n")
    }

    {
        let mut buf = vec![];
        write_tsv_escaped_string(b"123\n", &mut buf, b'\t');
        assert_eq!(&buf, b"123\\n")
    }

    {
        let mut buf = vec![];
        write_tsv_escaped_string(b"\n123", &mut buf, b'\t');
        assert_eq!(&buf, b"\\n123")
    }

    {
        let mut buf = vec![];
        write_tsv_escaped_string(b"\n,23", &mut buf, b',');
        assert_eq!(&buf, b"\\n\\,23")
    }
}

#[test]
fn test_json_escape() {
    let basic = b"\0\n\r\t\'/\"\\";
    {
        let mut buf = vec![];
        write_json_string(basic, &mut buf, false, true);
        assert_eq!(&buf, b"\\u0000\\n\\r\\t\'\\/\\\"\\\\")
    }

    {
        let mut buf = vec![];
        write_json_string(basic, &mut buf, false, false);
        assert_eq!(&buf, b"\\u0000\\n\\r\\t\'/\\\"\\\\")
    }

    {
        let mut buf = vec![];
        write_json_string(basic, &mut buf, true, true);
        assert_eq!(&buf, b"\\u0000\\n\\r\\t\'\\/\"\"\\\\")
    }

    {
        let mut buf = vec![];
        write_json_string(b"\n123\n456\n", &mut buf, true, true);
        assert_eq!(&buf, b"\\n123\\n456\\n")
    }

    {
        let mut buf = vec![];
        write_json_string(b"123\n", &mut buf, true, true);
        assert_eq!(&buf, b"123\\n")
    }

    {
        let mut buf = vec![];
        write_json_string(b"\n123", &mut buf, true, true);
        assert_eq!(&buf, b"\\n123")
    }

    {
        let mut buf = vec![];
        write_json_string(b"\n123", &mut buf, true, true);
        assert_eq!(&buf, b"\\n123")
    }

    {
        let s = "123\u{2028}\u{2029}abc";
        let mut buf = vec![];
        write_json_string(s.as_bytes(), &mut buf, true, true);
        assert_eq!(&buf, b"123\\u2028\\u2029abc")
    }
}

#[test]
fn test_csv_string() {
    {
        let s = "a\"\nb";
        let mut buf = vec![];
        write_csv_string(s.as_bytes(), &mut buf, b'"');
        assert_eq!(&buf, b"\"a\"\"\nb\"")
    }
}
