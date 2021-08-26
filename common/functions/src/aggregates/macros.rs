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

#[macro_export]
macro_rules! dispatch_numeric_types {
    ($dispatch: ident, $data_type: expr,  $($args:expr),*) => {
        $dispatch! { UInt8Type, $data_type,      $($args),* }
        $dispatch! { UInt16Type, $data_type,     $($args),* }
        $dispatch! { UInt32Type, $data_type,     $($args),* }
        $dispatch! { UInt64Type, $data_type,     $($args),* }
        $dispatch! { Int8Type, $data_type,       $($args),* }
        $dispatch! { Int16Type, $data_type,      $($args),* }
        $dispatch! { Int32Type, $data_type,      $($args),* }
        $dispatch! { Int64Type, $data_type,      $($args),* }
        $dispatch! { Float32Type, $data_type,    $($args),* }
        $dispatch! { Float64Type, $data_type,    $($args),* }
    };
}

#[macro_export]
macro_rules! dispatch_unsigned_numeric_types {
    ($dispatch: ident, $data_type: expr,  $($args:expr),*) => {
        $dispatch! { UInt8Type, $data_type,      $($args),* }
        $dispatch! { UInt16Type, $data_type,     $($args),* }
        $dispatch! { UInt32Type, $data_type,     $($args),* }
        $dispatch! { UInt64Type, $data_type,     $($args),* }
    };
}
