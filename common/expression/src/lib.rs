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

#![feature(generic_associated_types)]
#![feature(iterator_try_reduce)]
#![feature(box_patterns)]
#![feature(associated_type_defaults)]
#![allow(clippy::len_without_is_empty)]
#![allow(clippy::needless_lifetimes)]

pub mod chunk;
mod display;
pub mod evaluator;
pub mod expression;
pub mod function;
pub mod property;
pub mod type_check;
pub mod types;
pub mod util;
pub mod values;
