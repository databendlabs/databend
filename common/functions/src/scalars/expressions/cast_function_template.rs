use common_datavalues::DataType;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::CastFunction;
use crate::scalars::Function;

pub struct CastFunctionTemplate;

impl CastFunctionTemplate {
    pub fn try_create_func(display_name: &str) -> Result<Box<dyn Function>> {
        match display_name {
            "toint8" => CastFunction::create(DataType::Int8),
            "toint16" => CastFunction::create(DataType::Int16),
            "toint32" => CastFunction::create(DataType::Int32),
            "toint64" => CastFunction::create(DataType::Int64),
            _ => Err(ErrorCode::UnknownTableFunction("Function not found")),
        }
    }
}
