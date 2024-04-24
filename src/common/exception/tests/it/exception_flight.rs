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

use std::sync::Arc;

use arrow_format::flight::data::FlightData;
use backtrace::Backtrace;
use databend_common_exception::exception::ErrorCodeBacktrace;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

#[test]
fn test_serialize() -> Result<()> {
    let error_code = ErrorCode::create(
        1,
        "test_name",
        String::from("test_message"),
        String::new(),
        None,
        Some(ErrorCodeBacktrace::Symbols(Arc::new(Backtrace::new()))),
    )
    .set_span(Some((0..1).into()));
    let backtrace_str = error_code.backtrace_str();
    let error_code = ErrorCode::try_from(FlightData::from(error_code))?;
    assert_eq!(1, error_code.code());
    assert_eq!(String::from("test_name"), error_code.name());
    assert_eq!(String::from("test_message"), error_code.message());
    assert_eq!(backtrace_str, error_code.backtrace_str());
    assert_eq!(error_code.span(), Some((0..1).into()));
    Ok(())
}
