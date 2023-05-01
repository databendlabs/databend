// Copyright 2021 Datafuse Labs
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

mod csv;
mod fast_values;
mod json_ast;
mod row_based;
mod tsv;
mod values;
mod xml;

use std::any::Any;

pub use csv::FieldDecoderCSV;
pub use fast_values::FastFieldDecoderValues;
pub use json_ast::FieldJsonAstDecoder;
pub use row_based::FieldDecoderRowBased;
pub use tsv::FieldDecoderTSV;
pub use values::FieldDecoderValues;
pub use xml::FieldDecoderXML;

pub trait FieldDecoder: Send + Sync {
    fn as_any(&self) -> &dyn Any;
}
