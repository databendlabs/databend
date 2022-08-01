// Copyright 2021 Datafuse Labs.
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

use common_exception::Result;
use databend_query::sql::*;
use pretty_assertions::assert_eq;

#[test]
fn hint_test() -> Result<()> {
    {
        let comment = " { ErrorCode  1002 }";
        let expected = DfHint::create_from_comment(comment, "--");
        assert_eq!(expected.error_code, Some(1002));
    }

    {
        let comment = " { ErrorCode1002 }";
        let expected = DfHint::create_from_comment(comment, "--");
        assert_eq!(expected.error_code, None);
    }

    {
        let comment = " { ErrorCode 22}";
        let expected = DfHint::create_from_comment(comment, "--");
        assert_eq!(expected.error_code, Some(22));
    }

    {
        let comment = " { ErrorCode: 22}";
        let expected = DfHint::create_from_comment(comment, "--");
        assert_eq!(expected.error_code, None);
    }

    {
        let comment = " { Errorcode 22}";
        let expected = DfHint::create_from_comment(comment, "--");
        assert_eq!(expected.error_code, None);
    }

    Ok(())
}
