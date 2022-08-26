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
use std::collections::HashMap;
use std::sync::Arc;

use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::FormatSettings;
use once_cell::sync::Lazy;
use strum::IntoEnumIterator;

use super::format_tsv::TsvInputFormat;
use crate::format::InputFormat;
use crate::format_csv::CsvInputFormat;
use crate::format_ndjson::NDJsonInputFormat;
use crate::format_parquet::ParquetInputFormat;
use crate::output_format::OutputFormatType;

pub type InputFormatFactoryCreator =
    Box<dyn Fn(&str, DataSchemaRef, FormatSettings) -> Result<Arc<dyn InputFormat>> + Send + Sync>;

pub struct FormatFactory {
    case_insensitive_desc: HashMap<String, InputFormatFactoryCreator>,
    outputs: BTreeMap<String, OutputFormatType>,
}

static FORMAT_FACTORY: Lazy<Arc<FormatFactory>> = Lazy::new(|| {
    let mut format_factory = FormatFactory::create();

    CsvInputFormat::register(&mut format_factory);
    TsvInputFormat::register(&mut format_factory);
    ParquetInputFormat::register(&mut format_factory);
    NDJsonInputFormat::register(&mut format_factory);

    format_factory.register_outputs();
    Arc::new(format_factory)
});

impl FormatFactory {
    pub(in crate::format_factory) fn create() -> FormatFactory {
        FormatFactory {
            case_insensitive_desc: Default::default(),
            outputs: Default::default(),
        }
    }

    pub fn instance() -> &'static FormatFactory {
        FORMAT_FACTORY.as_ref()
    }

    pub fn register_input(&mut self, name: &str, creator: InputFormatFactoryCreator) {
        let case_insensitive_desc = &mut self.case_insensitive_desc;
        case_insensitive_desc.insert(name.to_lowercase(), creator);
    }

    pub fn has_input(&self, name: impl AsRef<str>) -> bool {
        let origin_name = name.as_ref();
        let lowercase_name = origin_name.to_lowercase();
        self.case_insensitive_desc.contains_key(&lowercase_name)
    }

    pub fn get_input(
        &self,
        name: impl AsRef<str>,
        schema: DataSchemaRef,
        settings: FormatSettings,
    ) -> Result<Arc<dyn InputFormat>> {
        let origin_name = name.as_ref();
        let lowercase_name = origin_name.to_lowercase();

        let creator = self
            .case_insensitive_desc
            .get(&lowercase_name)
            .ok_or_else(|| {
                ErrorCode::UnknownFormat(format!("Unsupported formats: {}", origin_name))
            })?;

        creator(origin_name, schema, settings)
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
