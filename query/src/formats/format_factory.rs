use std::collections::HashMap;
use std::sync::Arc;

use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::FormatSettings;
use once_cell::sync::Lazy;

use crate::formats::format::InputFormat;
use crate::formats::format_csv::CsvInputFormat;

pub type InputFormatFactoryCreator =
    Box<dyn Fn(&str, DataSchemaRef, FormatSettings) -> Result<Box<dyn InputFormat>> + Send + Sync>;

pub struct FormatFactory {
    case_insensitive_desc: HashMap<String, InputFormatFactoryCreator>,
}

static FORMAT_FACTORY: Lazy<Arc<FormatFactory>> = Lazy::new(|| {
    let mut format_factory = FormatFactory::create();

    CsvInputFormat::register(&mut format_factory);

    Arc::new(format_factory)
});

impl FormatFactory {
    pub(in crate::formats::format_factory) fn create() -> FormatFactory {
        FormatFactory {
            case_insensitive_desc: Default::default(),
        }
    }

    pub fn instance() -> &'static FormatFactory {
        FORMAT_FACTORY.as_ref()
    }

    pub fn register_input(&mut self, name: &str, creator: InputFormatFactoryCreator) {
        let case_insensitive_desc = &mut self.case_insensitive_desc;
        case_insensitive_desc.insert(name.to_lowercase(), creator);
    }

    pub fn get_input(
        &self,
        name: impl AsRef<str>,
        schema: DataSchemaRef,
        settings: FormatSettings,
    ) -> Result<Box<dyn InputFormat>> {
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
}
