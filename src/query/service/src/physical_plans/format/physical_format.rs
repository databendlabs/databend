use databend_common_ast::ast::FormatTreeNode;
use databend_common_exception::Result;

use crate::physical_plans::format::FormatContext;

pub trait PhysicalFormat {
    fn dispatch(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        self.format(ctx)
    }

    fn format(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>>;
}
