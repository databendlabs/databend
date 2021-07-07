// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;

use common_datavalues::DataSchema;

use crate::plan_display_indent::PlanNodeIndentFormatDisplay;
use crate::Expression;
use crate::PlanNode;

impl PlanNode {
    pub fn display_indent_format(&self) -> impl fmt::Display + '_ {
        PlanNodeIndentFormatDisplay::create(0, self, false)
    }

    pub fn display_graphviz(&self) -> impl fmt::Display + '_ {
        struct Wrapper<'a>(&'a PlanNode);
        impl<'a> fmt::Display for Wrapper<'a> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                writeln!(
                    f,
                    "// Begin DataFuse GraphViz Plan (see https://graphviz.org)"
                )?;
                writeln!(f, "digraph {{")?;
                // TODO()
                writeln!(f, "}}")?;
                writeln!(f, "// End DataFuse GraphViz Plan")?;
                Ok(())
            }
        }
        Wrapper(self)
    }

    pub fn display_schema(schema: &DataSchema) -> impl fmt::Display + '_ {
        struct Wrapper<'a>(&'a DataSchema);

        impl<'a> fmt::Display for Wrapper<'a> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "[")?;
                for (idx, field) in self.0.fields().iter().enumerate() {
                    if idx > 0 {
                        write!(f, ", ")?;
                    }
                    let nullable_str = if field.is_nullable() { ";N" } else { "" };
                    write!(
                        f,
                        "{}:{:?}{}",
                        field.name(),
                        field.data_type(),
                        nullable_str
                    )?;
                }
                write!(f, "]")
            }
        }
        Wrapper(schema)
    }
}

impl fmt::Debug for PlanNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.display_indent_format().fmt(f)
    }
}
