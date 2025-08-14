use databend_common_ast::ast::FormatTreeNode;
use databend_common_exception::Result;

use crate::physical_plans::format::FormatContext;

pub trait PhysicalFormat {
    fn format(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>>;
}
