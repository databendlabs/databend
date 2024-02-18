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

use chrono::DateTime;
use chrono::Utc;
use databend_common_expression::infer_schema_type;
use databend_common_expression::types::DataType;
use databend_common_expression::TableDataType;
use databend_common_meta_app::principal as mt;
use databend_common_protos::pb;

use crate::reader_check_msg;
use crate::FromToProto;
use crate::Incompatible;
use crate::MIN_READER_VER;
use crate::VER;

impl FromToProto for mt::LambdaUDF {
    type PB = pb::LambdaUdf;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::LambdaUdf) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        Ok(mt::LambdaUDF {
            parameters: p.parameters,
            definition: p.definition,
        })
    }

    fn to_pb(&self) -> Result<pb::LambdaUdf, Incompatible> {
        Ok(pb::LambdaUdf {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            parameters: self.parameters.clone(),
            definition: self.definition.clone(),
        })
    }
}

impl FromToProto for mt::UDFServer {
    type PB = pb::UdfServer;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::UdfServer) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let mut arg_types = Vec::with_capacity(p.arg_types.len());
        for arg_type in p.arg_types {
            let arg_type = DataType::from(&TableDataType::from_pb(arg_type)?);
            arg_types.push(arg_type);
        }
        let return_type = DataType::from(&TableDataType::from_pb(p.return_type.ok_or_else(
            || Incompatible {
                reason: "UdfServer.return_type can not be None".to_string(),
            },
        )?)?);

        Ok(mt::UDFServer {
            address: p.address,
            arg_types,
            return_type,
            handler: p.handler,
            language: p.language,
        })
    }

    fn to_pb(&self) -> Result<pb::UdfServer, Incompatible> {
        let mut arg_types = Vec::with_capacity(self.arg_types.len());
        for arg_type in self.arg_types.iter() {
            let arg_type = infer_schema_type(arg_type)
                .map_err(|e| Incompatible {
                    reason: format!("Convert DataType to TableDataType failed: {}", e.message()),
                })?
                .to_pb()?;
            arg_types.push(arg_type);
        }
        let return_type = infer_schema_type(&self.return_type)
            .map_err(|e| Incompatible {
                reason: format!("Convert DataType to TableDataType failed: {}", e.message()),
            })?
            .to_pb()?;

        Ok(pb::UdfServer {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            address: self.address.clone(),
            handler: self.handler.clone(),
            language: self.language.clone(),
            arg_types,
            return_type: Some(return_type),
        })
    }
}

impl FromToProto for mt::UserDefinedFunction {
    type PB = pb::UserDefinedFunction;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::UserDefinedFunction) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;
        let udf_def = match p.definition {
            Some(pb::user_defined_function::Definition::LambdaUdf(lambda_udf)) => {
                mt::UDFDefinition::LambdaUDF(mt::LambdaUDF::from_pb(lambda_udf)?)
            }
            Some(pb::user_defined_function::Definition::UdfServer(udf_server)) => {
                mt::UDFDefinition::UDFServer(mt::UDFServer::from_pb(udf_server)?)
            }
            None => {
                return Err(Incompatible {
                    reason: "UserDefinedFunction.definition cannot be None".to_string(),
                });
            }
        };

        Ok(mt::UserDefinedFunction {
            name: p.name,
            description: p.description,
            definition: udf_def,
            created_on: match p.created_on {
                Some(c) => DateTime::<Utc>::from_pb(c)?,
                None => DateTime::<Utc>::default(),
            },
        })
    }

    fn to_pb(&self) -> Result<pb::UserDefinedFunction, Incompatible> {
        let udf_def = match &self.definition {
            mt::UDFDefinition::LambdaUDF(lambda_udf) => {
                pb::user_defined_function::Definition::LambdaUdf(lambda_udf.to_pb()?)
            }
            mt::UDFDefinition::UDFServer(udf_server) => {
                pb::user_defined_function::Definition::UdfServer(udf_server.to_pb()?)
            }
        };

        Ok(pb::UserDefinedFunction {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            name: self.name.clone(),
            description: self.description.clone(),
            definition: Some(udf_def),
            created_on: Some(self.created_on.to_pb()?),
        })
    }
}
