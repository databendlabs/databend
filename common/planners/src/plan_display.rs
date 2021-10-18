// Copyright 2020 Datafuse Labs.
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

use std::fmt;
use std::fmt::Display;

use common_datavalues::DataSchema;

use crate::plan_display_indent::PlanNodeIndentFormatDisplay;
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
                    "// Begin Databend GraphViz Plan (see https://graphviz.org)"
                )?;
                writeln!(f, "digraph {{")?;
                // TODO()
                writeln!(f, "}}")?;
                writeln!(f, "// End Databend GraphViz Plan")?;
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
