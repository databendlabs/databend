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

use std::collections::BTreeMap;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use once_cell::sync::Lazy;
use strum::IntoEnumIterator;

use crate::output_format::OutputFormatType;

pub struct FormatFactory {
    outputs: BTreeMap<String, OutputFormatType>,
}

static FORMAT_FACTORY: Lazy<Arc<FormatFactory>> = Lazy::new(|| {
    let mut format_factory = FormatFactory::create();

    format_factory.register_outputs();
    Arc::new(format_factory)
});

impl FormatFactory {
    pub(in crate::format_factory) fn create() -> FormatFactory {
        FormatFactory {
            outputs: Default::default(),
        }
    }

    pub fn instance() -> &'static FormatFactory {
        FORMAT_FACTORY.as_ref()
    }

    pub fn get_output(&self, name: &str) -> Result<OutputFormatType> {
        self.outputs
            .get(&name.to_lowercase())
            .cloned()
            .ok_or_else(|| ErrorCode::UnknownFormat(format!("Unsupported formats: {}", name)))
    }

    fn register_output(&mut self, typ: OutputFormatType) {
        let base_name = format!("{:?}", typ);
        let mut names = typ.base_alias();
        names.push(base_name);
        for n in &names {
            self.register_output_by_name(n.clone(), typ);
            if let Some(t) = typ.with_names() {
                self.register_output_by_name(n.to_string() + "WithNames", t);
            }
            if let Some(t) = typ.with_names_and_types() {
                self.register_output_by_name(n.to_string() + "WithNamesAndTypes", t);
            }
        }
    }

    fn register_output_by_name(&mut self, name: String, typ: OutputFormatType) {
        self.outputs.insert(name.to_lowercase(), typ);
    }

    fn register_outputs(&mut self) {
        for t in OutputFormatType::iter() {
            self.register_output(t)
        }
    }
}
