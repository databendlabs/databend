// Copyright 2021 Datafuse Labs
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

use crate::ast::*;
use crate::visit::VisitControl;
use crate::visit::Visitor;
use crate::visit::VisitorMut;
use crate::visit::Walk;
use crate::visit::WalkMut;

impl Walk for UDFDefinition {
    fn walk<V: Visitor + ?Sized>(
        &self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        match self {
            UDFDefinition::LambdaUDF {
                parameters,
                definition,
            } => {
                match parameters {
                    LambdaUDFParams::Names(names) => {
                        for ident in names {
                            try_walk!(ident.walk(visitor));
                        }
                    }
                    LambdaUDFParams::NameWithTypes(pairs) => {
                        try_walk!(pairs.walk(visitor));
                    }
                }
                try_walk!(definition.walk(visitor));
            }
            UDFDefinition::UDFServer {
                arg_types,
                return_type,
                ..
            }
            | UDFDefinition::UDFScript {
                arg_types,
                return_type,
                ..
            } => {
                try_walk!(arg_types.walk(visitor));
                try_walk!(return_type.walk(visitor));
            }
            UDFDefinition::UDAFServer {
                arg_types,
                state_fields,
                return_type,
                ..
            }
            | UDFDefinition::UDAFScript {
                arg_types,
                state_fields,
                return_type,
                ..
            } => {
                try_walk!(arg_types.walk(visitor));
                for field in state_fields {
                    try_walk!(field.name.walk(visitor));
                    try_walk!(field.type_name.walk(visitor));
                }
                try_walk!(return_type.walk(visitor));
            }
            UDFDefinition::UDTFSql {
                arg_types,
                return_types,
                ..
            }
            | UDFDefinition::UDTFServer {
                arg_types,
                return_types,
                ..
            } => {
                try_walk!(arg_types.walk(visitor));
                try_walk!(return_types.walk(visitor));
            }
            UDFDefinition::ScalarUDF {
                arg_types,
                return_type,
                ..
            } => {
                try_walk!(arg_types.walk(visitor));
                try_walk!(return_type.walk(visitor));
            }
        }
        Ok(VisitControl::Continue)
    }
}

impl WalkMut for UDFDefinition {
    fn walk_mut<V: VisitorMut + ?Sized>(
        &mut self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        match self {
            UDFDefinition::LambdaUDF {
                parameters,
                definition,
            } => {
                match parameters {
                    LambdaUDFParams::Names(names) => {
                        for ident in names {
                            try_walk!(ident.walk_mut(visitor));
                        }
                    }
                    LambdaUDFParams::NameWithTypes(pairs) => {
                        try_walk!(pairs.walk_mut(visitor));
                    }
                }
                try_walk!(definition.walk_mut(visitor));
            }
            UDFDefinition::UDFServer {
                arg_types,
                return_type,
                ..
            }
            | UDFDefinition::UDFScript {
                arg_types,
                return_type,
                ..
            } => {
                try_walk!(arg_types.walk_mut(visitor));
                try_walk!(return_type.walk_mut(visitor));
            }
            UDFDefinition::UDAFServer {
                arg_types,
                state_fields,
                return_type,
                ..
            }
            | UDFDefinition::UDAFScript {
                arg_types,
                state_fields,
                return_type,
                ..
            } => {
                try_walk!(arg_types.walk_mut(visitor));
                for field in state_fields {
                    try_walk!(field.name.walk_mut(visitor));
                    try_walk!(field.type_name.walk_mut(visitor));
                }
                try_walk!(return_type.walk_mut(visitor));
            }
            UDFDefinition::UDTFSql {
                arg_types,
                return_types,
                ..
            }
            | UDFDefinition::UDTFServer {
                arg_types,
                return_types,
                ..
            } => {
                try_walk!(arg_types.walk_mut(visitor));
                try_walk!(return_types.walk_mut(visitor));
            }
            UDFDefinition::ScalarUDF {
                arg_types,
                return_type,
                ..
            } => {
                try_walk!(arg_types.walk_mut(visitor));
                try_walk!(return_type.walk_mut(visitor));
            }
        }
        Ok(VisitControl::Continue)
    }
}

impl Walk for CreateTaskStmt {
    fn walk<V: Visitor + ?Sized>(
        &self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        if let Some(when_condition) = &self.when_condition {
            try_walk!(when_condition.walk(visitor));
        }
        Ok(VisitControl::Continue)
    }
}

impl WalkMut for CreateTaskStmt {
    fn walk_mut<V: VisitorMut + ?Sized>(
        &mut self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        if let Some(when_condition) = &mut self.when_condition {
            try_walk!(when_condition.walk_mut(visitor));
        }
        Ok(VisitControl::Continue)
    }
}

impl Walk for AlterTaskStmt {
    fn walk<V: Visitor + ?Sized>(
        &self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        if let AlterTaskOptions::ModifyWhen(expr) = &self.options {
            try_walk!(expr.walk(visitor));
        }
        Ok(VisitControl::Continue)
    }
}

impl WalkMut for AlterTaskStmt {
    fn walk_mut<V: VisitorMut + ?Sized>(
        &mut self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        if let AlterTaskOptions::ModifyWhen(expr) = &mut self.options {
            try_walk!(expr.walk_mut(visitor));
        }
        Ok(VisitControl::Continue)
    }
}
