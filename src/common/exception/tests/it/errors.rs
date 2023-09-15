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

use common_exception::ErrorCode;
use common_exception::Errors;

#[test]
fn test_errors() {
    // Have defined error code.
    {
        let error_code = ErrorCode::BadBytes("bad bytes");
        let error = Errors::get(error_code);

        assert_eq!(
            error,
            "{\"name\":\"BadBytes\",\"code\":1046,\"message\":\"Content included bad bytes: bad bytes\"}"
        );
    }

    // Not defined error code.
    {
        let error_code = ErrorCode::AbortedQuery("abort query");
        let error = Errors::get(error_code);

        assert_eq!(
            error,
            "{\"name\":\"AbortedQuery\",\"code\":1043,\"message\":\"abort query\"}"
        );
    }
}
