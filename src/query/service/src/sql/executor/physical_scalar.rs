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
use common_exception::Result;

use super::ColumnID;
use crate::sql::IndexType;
use crate::sql::MetadataRef;

/// Serializable and desugared representation of `Scalar`.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum PhysicalScalar {
    Variable {
        column_id: ColumnID,
        data_type: DataTypeImpl,
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
            PhysicalScalar::Variable { data_type, .. } => data_type.clone(),
            PhysicalScalar::Constant { data_type, .. } => data_type.clone(),
            PhysicalScalar::Function { return_type, .. } => return_type.clone(),
            PhysicalScalar::Cast { target, .. } => target.clone(),
        }
    }

    /// Display with readable variable name.
    pub fn pretty_display(&self, metadata: &MetadataRef) -> Result<String> {
        match self {
            PhysicalScalar::Variable { column_id, .. } => {
                let index = column_id.parse::<IndexType>()?;
                let column = metadata.read().column(index).clone();
                let table_name = match column.table_index {
                    Some(table_index) => {
                        format!("{}.", metadata.read().table(table_index).name.clone())
                    }
                    None => "".to_string(),
                };
                Ok(format!("{}{} (#{})", table_name, column.name, index))
            }
            PhysicalScalar::Constant { value, .. } => Ok(value.to_string()),
            PhysicalScalar::Function { name, args, .. } => {
                let args = args
                    .iter()
                    .map(|(arg, _)| arg.pretty_display(metadata))
                    .collect::<Result<Vec<_>>>()?
                    .join(", ");
                Ok(format!("{}({})", name, args))
            }
            PhysicalScalar::Cast { input, target } => Ok(format!(
                "CAST({} AS {})",
                input.pretty_display(metadata)?,
                format_data_type_sql(target)
            )),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AggregateFunctionDesc {
    pub sig: AggregateFunctionSignature,
    pub column_id: ColumnID,
    pub args: Vec<ColumnID>,
}

impl AggregateFunctionDesc {
    pub fn pretty_display(&self, metadata: &MetadataRef) -> Result<String> {
        Ok(format!(
            "{}({})",
            self.sig.name,
            self.args
                .iter()
                .map(|arg| {
                    let index = arg.parse::<IndexType>()?;
                    let column = metadata.read().column(index).clone();
                    Ok(column.name)
                })
                .collect::<Result<Vec<_>>>()?
                .join(", ")
        ))
    }
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
