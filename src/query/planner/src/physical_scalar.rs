// Copyright 2022 Datafuse Labs.
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

use common_datavalues::format_data_type_sql;
use common_datavalues::DataTypeImpl;
use common_datavalues::DataValue;

type ColumnID = String;
type IndexType = usize;

/// Serializable and desugared representation of `Scalar`.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum PhysicalScalar {
    IndexedVariable {
        index: usize,
        data_type: DataTypeImpl,

        display_name: String,
    },
    Constant {
        value: DataValue,
        data_type: DataTypeImpl,
    },
    Function {
        name: String,
        args: Vec<(PhysicalScalar, DataTypeImpl)>,
        return_type: DataTypeImpl,
    },

    Cast {
        input: Box<PhysicalScalar>,
        target: DataTypeImpl,
    },
}

impl PhysicalScalar {
    pub fn data_type(&self) -> DataTypeImpl {
        match self {
            PhysicalScalar::Constant { data_type, .. } => data_type.clone(),
            PhysicalScalar::Function { return_type, .. } => return_type.clone(),
            PhysicalScalar::Cast { target, .. } => target.clone(),
            PhysicalScalar::IndexedVariable { data_type, .. } => data_type.clone(),
        }
    }

    /// Display with readable variable name.
    pub fn pretty_display(&self) -> String {
        match self {
            PhysicalScalar::Constant { value, .. } => value.to_string(),
            PhysicalScalar::Function { name, args, .. } => {
                let args = args
                    .iter()
                    .map(|(arg, _)| arg.pretty_display())
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{}({})", name, args)
            }
            PhysicalScalar::Cast { input, target } => format!(
                "CAST({} AS {})",
                input.pretty_display(),
                format_data_type_sql(target)
            ),
            PhysicalScalar::IndexedVariable { display_name, .. } => display_name.clone(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AggregateFunctionDesc {
    pub sig: AggregateFunctionSignature,
    pub column_id: ColumnID,
    pub args: Vec<usize>,

    /// Only used for debugging
    pub arg_indices: Vec<IndexType>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AggregateFunctionSignature {
    pub name: String,
    pub args: Vec<DataTypeImpl>,
    pub params: Vec<DataValue>,
    pub return_type: DataTypeImpl,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SortDesc {
    pub asc: bool,
    pub nulls_first: bool,
    pub order_by: ColumnID,
}
