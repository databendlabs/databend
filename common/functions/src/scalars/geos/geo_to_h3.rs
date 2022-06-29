use std::fmt::Display;
use std::fmt::Formatter;

use common_datavalues::ColumnRef;
use common_datavalues::ColumnsWithField;
use common_datavalues::DataTypeImpl;
use common_datavalues::TypeID;
use common_exception::ErrorCode;

use crate::scalars::Function;
use crate::scalars::FunctionContext;

#[derive(Clone)]
pub struct GeoToH3Function {
    display_name: String,
}

impl Function for GeoToH3Function {
    fn name(&self) -> &str {
        &self.display_name
    }

    fn return_type(&self) -> DataTypeImpl {
        UInt64Type::new_impl()
    }

    fn eval(
        &self,
        _func_ctx: FunctionContext,
        columns: &ColumnsWithField,
        _input_rows: usize,
    ) -> common_exception::Result<ColumnRef> {
        assert_eq!(
            3,
            columns.len(),
            "args of function {} must be 3",
            self.name()
        );

        // check datatype
        let mut col = columns[0].column();
        if col.data_type_id() != TypeID::Float64 {
            return Err(ErrorCode::BadDataValueType(format!(
                "Invalid type {} of argument {} for function '{}'. Must be Float64",
                col.data_type_id(),
                1,
                self.name(),
            )));
        }

        Ok(());
    }
}

impl Display for GeoToH3Function {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.display_name.to_uppercase())
    }
}
