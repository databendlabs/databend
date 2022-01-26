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
#![feature(io_error_other)]
#![feature(mixed_integer_ops)]
#![feature(type_alias_impl_trait)]
#![feature(associated_type_defaults)]

mod accessor;
pub use accessor::Accessor;
pub use accessor::Reader;

mod operator;
pub use operator::Operator;

pub mod credential;
pub mod error;
pub mod ops;
pub mod services;
pub mod wraps;
