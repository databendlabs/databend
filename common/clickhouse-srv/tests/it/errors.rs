// Copyright 2020 Datafuse Labs.
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

use common_clickhouse_srv::errors::*;

#[test]
fn to_std_error_without_recursion() {
    let src_err: Error = From::from("Somth went wrong.");
    let dst_err: Box<dyn std::error::Error> = src_err.into();
    assert_eq!(dst_err.to_string(), "Other error: `Somth went wrong.`");
}

#[test]
fn to_io_error_without_recursion() {
    let src_err: Error = From::from("Somth went wrong.");
    let dst_err: std::io::Error = src_err.into();
    assert_eq!(dst_err.to_string(), "Other error: `Somth went wrong.`");
}
