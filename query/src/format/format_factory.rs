use std::collections::HashMap;
use std::sync::Arc;
use once_cell::sync::Lazy;
use crate::InputFormat;
use common_exception::{ErrorCode, Result};
use crate::format::format::InputFormat;

pub type InputFormatFactoryCreator = Box<dyn Fn(&str) -> Result<Box<dyn InputFormat>> + Send + Sync>;

pub struct FormatFactory {
    case_insensitive_desc: HashMap<String, InputFormatFactoryCreator>,
}

static FORMAT_FACTORY: Lazy<Arc<FormatFactory>> = Lazy::new(|| {
    let mut format_factory = FormatFactory::create();

    Arc::new(format_factory)
});

impl FormatFactory {
    pub(in crate::format::format_factory) fn create() -> FormatFactory {
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

    pub fn get_input(&self, name: impl AsRef<str>) -> Result<Box<dyn InputFormat>> {
        let origin_name = name.as_ref();
        let lowercase_name = origin_name.to_lowercase();

        let creator = self
            .case_insensitive_desc
            .get(&lowercase_name)
            .ok_or_else(|| ErrorCode::UnknownFormat(format!("Unsupported format: {}", origin_name)))?;


        creator(&origin_name)
    }
}

