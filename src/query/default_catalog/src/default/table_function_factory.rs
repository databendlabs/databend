use std::sync::Arc;

use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_function::TableFunction;
use databend_common_exception::Result;
pub trait TableFunctionFactory {
    fn get(&self, func_name: &str, tbl_args: TableArgs) -> Result<Arc<dyn TableFunction>>;
    fn exists(&self, func_name: &str) -> bool;
    fn list(&self) -> Vec<String>;
}
