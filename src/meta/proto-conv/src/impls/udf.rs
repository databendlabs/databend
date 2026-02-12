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

use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::infer_schema_type;
use databend_common_expression::types::DataType;
use databend_common_meta_app::principal as mt;
use databend_common_protos::pb;
use databend_common_protos::pb::UdtfArg;

use crate::FromToProto;
use crate::Incompatible;
use crate::MIN_READER_VER;
use crate::VER;
use crate::reader_check_msg;

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
            || Incompatible::new("UdfServer.return_type can not be None".to_string()),
        )?)?);

        Ok(mt::UDFServer {
            address: p.address,
            arg_types,
            return_type,
            handler: p.handler,
            headers: p.headers,
            language: p.language,
            immutable: p.immutable,
            arg_names: p.arg_names,
        })
    }

    fn to_pb(&self) -> Result<pb::UdfServer, Incompatible> {
        let mut arg_types = Vec::with_capacity(self.arg_types.len());
        for arg_type in self.arg_types.iter() {
            let arg_type = infer_schema_type(arg_type)
                .map_err(|e| {
                    Incompatible::new(format!(
                        "Convert DataType to TableDataType failed: {}",
                        e.message()
                    ))
                })?
                .to_pb()?;
            arg_types.push(arg_type);
        }
        let return_type = infer_schema_type(&self.return_type)
            .map_err(|e| {
                Incompatible::new(format!(
                    "Convert DataType to TableDataType failed: {}",
                    e.message()
                ))
            })?
            .to_pb()?;

        Ok(pb::UdfServer {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            address: self.address.clone(),
            handler: self.handler.clone(),
            headers: self.headers.clone(),
            language: self.language.clone(),
            arg_types,
            return_type: Some(return_type),
            immutable: self.immutable,
            arg_names: self.arg_names.clone(),
        })
    }
}

impl FromToProto for mt::UDFScript {
    type PB = pb::UdfScript;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::UdfScript) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let mut arg_types = Vec::with_capacity(p.arg_types.len());
        for arg_type in p.arg_types {
            let arg_type = DataType::from(&TableDataType::from_pb(arg_type)?);
            arg_types.push(arg_type);
        }
        let return_type = DataType::from(&TableDataType::from_pb(p.return_type.ok_or_else(
            || Incompatible::new("UDFScript.return_type can not be None".to_string()),
        )?)?);

        Ok(mt::UDFScript {
            code: p.code,
            arg_types,
            return_type,
            handler: p.handler,
            language: p.language,
            runtime_version: p.runtime_version,
            imports: p.imports,
            packages: p.packages,
            immutable: p.immutable,
        })
    }

    fn to_pb(&self) -> Result<pb::UdfScript, Incompatible> {
        let mut arg_types = Vec::with_capacity(self.arg_types.len());
        for arg_type in self.arg_types.iter() {
            let arg_type = infer_schema_type(arg_type)
                .map_err(|e| {
                    Incompatible::new(format!(
                        "Convert DataType to TableDataType failed: {}",
                        e.message()
                    ))
                })?
                .to_pb()?;
            arg_types.push(arg_type);
        }
        let return_type = infer_schema_type(&self.return_type)
            .map_err(|e| {
                Incompatible::new(format!(
                    "Convert DataType to TableDataType failed: {}",
                    e.message()
                ))
            })?
            .to_pb()?;

        Ok(pb::UdfScript {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            code: self.code.clone(),
            handler: self.handler.clone(),
            language: self.language.clone(),
            arg_types,
            return_type: Some(return_type),
            runtime_version: self.runtime_version.clone(),
            imports: self.imports.clone(),
            packages: self.packages.clone(),
            immutable: self.immutable,
        })
    }
}

impl FromToProto for mt::UDAFScript {
    type PB = pb::UdafScript;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::UdafScript) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let arg_types = p
            .arg_types
            .into_iter()
            .map(|arg_type| Ok((&TableDataType::from_pb(arg_type)?).into()))
            .collect::<Result<Vec<_>, _>>()?;

        let state_fields = p
            .state_fields
            .into_iter()
            .map(|field| TableField::from_pb(field).map(|field| (&field).into()))
            .collect::<Result<Vec<_>, _>>()?;

        let return_type = (&TableDataType::from_pb(p.return_type.ok_or_else(|| {
            Incompatible::new("UDAFScript.return_type can not be None".to_string())
        })?)?)
            .into();

        Ok(mt::UDAFScript {
            code: p.code,
            arg_types,
            return_type,
            language: p.language,
            runtime_version: p.runtime_version,
            imports: p.imports,
            packages: p.packages,
            state_fields,
        })
    }

    fn to_pb(&self) -> Result<pb::UdafScript, Incompatible> {
        let mut arg_types = Vec::with_capacity(self.arg_types.len());
        for arg_type in self.arg_types.iter() {
            let arg_type = infer_schema_type(arg_type)
                .map_err(|e| {
                    Incompatible::new(format!(
                        "Convert DataType to TableDataType failed: {}",
                        e.message()
                    ))
                })?
                .to_pb()?;
            arg_types.push(arg_type);
        }

        let state_fields = self
            .state_fields
            .iter()
            .map(|field| {
                TableField::new(
                    field.name(),
                    infer_schema_type(field.data_type()).map_err(|e| {
                        Incompatible::new(format!(
                            "Convert DataType to TableDataType failed: {}",
                            e.message()
                        ))
                    })?,
                )
                .to_pb()
            })
            .collect::<Result<_, _>>()?;

        let return_type = infer_schema_type(&self.return_type)
            .map_err(|e| {
                Incompatible::new(format!(
                    "Convert DataType to TableDataType failed: {}",
                    e.message()
                ))
            })?
            .to_pb()?;

        Ok(pb::UdafScript {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            code: self.code.clone(),
            language: self.language.clone(),
            runtime_version: self.runtime_version.clone(),
            arg_types,
            state_fields,
            return_type: Some(return_type),
            imports: self.imports.clone(),
            packages: self.packages.clone(),
        })
    }
}

impl FromToProto for mt::UDTF {
    type PB = pb::Udtf;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let mut arg_types = Vec::new();
        for arg_ty in p.arg_types {
            let ty_pb = arg_ty.ty.ok_or_else(|| {
                Incompatible::new("UDTF.arg_types.ty can not be None".to_string())
            })?;
            let ty = TableDataType::from_pb(ty_pb)?;

            arg_types.push((arg_ty.name, (&ty).into()));
        }

        let mut return_types = Vec::new();
        for return_ty in p.return_types {
            let ty_pb = return_ty.ty.ok_or_else(|| {
                Incompatible::new("UDTF.arg_types.ty can not be None".to_string())
            })?;
            let ty = TableDataType::from_pb(ty_pb)?;

            return_types.push((return_ty.name, (&ty).into()));
        }

        Ok(Self {
            arg_types,
            return_types,
            sql: p.sql,
        })
    }

    fn to_pb(&self) -> Result<Self::PB, Incompatible> {
        let mut arg_types = Vec::with_capacity(self.arg_types.len());
        for (arg_name, arg_type) in self.arg_types.iter() {
            let arg_type = infer_schema_type(arg_type)
                .map_err(|e| {
                    Incompatible::new(format!(
                        "Convert DataType to TableDataType failed: {}",
                        e.message()
                    ))
                })?
                .to_pb()?;
            arg_types.push(UdtfArg {
                name: arg_name.clone(),
                ty: Some(arg_type),
            });
        }

        let mut return_types = Vec::with_capacity(self.return_types.len());
        for (return_name, return_type) in self.return_types.iter() {
            let return_type = infer_schema_type(return_type)
                .map_err(|e| {
                    Incompatible::new(format!(
                        "Convert DataType to TableDataType failed: {}",
                        e.message()
                    ))
                })?
                .to_pb()?;
            return_types.push(UdtfArg {
                name: return_name.clone(),
                ty: Some(return_type),
            });
        }

        Ok(pb::Udtf {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            arg_types,
            return_types,
            sql: self.sql.clone(),
        })
    }
}

impl FromToProto for mt::UDTFServer {
    type PB = pb::UdtfServer;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let mut arg_types = Vec::with_capacity(p.arg_types.len());
        for arg_type in p.arg_types {
            let arg_type = DataType::from(&TableDataType::from_pb(arg_type)?);
            arg_types.push(arg_type);
        }
        let mut return_types = Vec::new();
        for return_ty in p.return_types {
            let ty_pb = return_ty.ty.ok_or_else(|| {
                Incompatible::new("UDTF.arg_types.ty can not be None".to_string())
            })?;
            let ty = TableDataType::from_pb(ty_pb)?;

            return_types.push((return_ty.name, (&ty).into()));
        }

        Ok(mt::UDTFServer {
            address: p.address,
            arg_types,
            return_types,
            handler: p.handler,
            headers: p.headers,
            language: p.language,
            immutable: p.immutable,
            arg_names: p.arg_names,
        })
    }

    fn to_pb(&self) -> Result<Self::PB, Incompatible> {
        let mut arg_types = Vec::with_capacity(self.arg_types.len());
        for arg_type in self.arg_types.iter() {
            let arg_type = infer_schema_type(arg_type)
                .map_err(|e| {
                    Incompatible::new(format!(
                        "Convert DataType to TableDataType failed: {}",
                        e.message()
                    ))
                })?
                .to_pb()?;
            arg_types.push(arg_type);
        }
        let mut return_types = Vec::with_capacity(self.return_types.len());
        for (return_name, return_type) in self.return_types.iter() {
            let return_type = infer_schema_type(return_type)
                .map_err(|e| {
                    Incompatible::new(format!(
                        "Convert DataType to TableDataType failed: {}",
                        e.message()
                    ))
                })?
                .to_pb()?;
            return_types.push(UdtfArg {
                name: return_name.clone(),
                ty: Some(return_type),
            });
        }

        Ok(pb::UdtfServer {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            address: self.address.clone(),
            handler: self.handler.clone(),
            headers: self.headers.clone(),
            language: self.language.clone(),
            arg_types,
            return_types,
            immutable: self.immutable,
            arg_names: self.arg_names.clone(),
        })
    }
}

impl FromToProto for mt::ScalarUDF {
    type PB = pb::ScalarUdf;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let mut arg_types = Vec::new();
        for arg_ty in p.arg_types {
            let ty_pb = arg_ty.ty.ok_or_else(|| {
                Incompatible::new("ScalarUDF.arg_types.ty can not be None".to_string())
            })?;
            let ty = TableDataType::from_pb(ty_pb)?;

            arg_types.push((arg_ty.name, (&ty).into()));
        }

        let return_type_pb = p.return_type.ok_or_else(|| {
            Incompatible::new("ScalarUDF.return_type can not be None".to_string())
        })?;
        let return_type = TableDataType::from_pb(return_type_pb)?;

        Ok(Self {
            arg_types,
            return_type: (&return_type).into(),
            definition: p.definition,
        })
    }

    fn to_pb(&self) -> Result<Self::PB, Incompatible> {
        let mut arg_types = Vec::with_capacity(self.arg_types.len());
        for (arg_name, arg_type) in self.arg_types.iter() {
            let arg_type = infer_schema_type(arg_type)
                .map_err(|e| {
                    Incompatible::new(format!(
                        "Convert DataType to TableDataType failed: {}",
                        e.message()
                    ))
                })?
                .to_pb()?;
            arg_types.push(UdtfArg {
                name: arg_name.clone(),
                ty: Some(arg_type),
            });
        }

        let return_type = infer_schema_type(&self.return_type)
            .map_err(|e| {
                Incompatible::new(format!(
                    "Convert DataType to TableDataType failed: {}",
                    e.message()
                ))
            })?
            .to_pb()?;

        Ok(pb::ScalarUdf {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            arg_types,
            return_type: Some(return_type),
            definition: self.definition.clone(),
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
            Some(pb::user_defined_function::Definition::UdfScript(udf_script)) => {
                mt::UDFDefinition::UDFScript(mt::UDFScript::from_pb(udf_script)?)
            }
            Some(pb::user_defined_function::Definition::UdafScript(udaf_script)) => {
                mt::UDFDefinition::UDAFScript(mt::UDAFScript::from_pb(udaf_script)?)
            }
            Some(pb::user_defined_function::Definition::Udtf(udtf)) => {
                mt::UDFDefinition::UDTF(mt::UDTF::from_pb(udtf)?)
            }
            Some(pb::user_defined_function::Definition::ScalarUdf(scalar_udf)) => {
                mt::UDFDefinition::ScalarUDF(mt::ScalarUDF::from_pb(scalar_udf)?)
            }
            Some(pb::user_defined_function::Definition::UdtfServer(udtf_server)) => {
                mt::UDFDefinition::UDTFServer(mt::UDTFServer::from_pb(udtf_server)?)
            }
            None => {
                return Err(Incompatible::new(
                    "UserDefinedFunction.definition cannot be None".to_string(),
                ));
            }
        };

        let created_on = p
            .created_on
            .map(FromToProto::from_pb)
            .transpose()?
            .unwrap_or_default();
        let update_on = p
            .update_on
            .map(FromToProto::from_pb)
            .transpose()?
            .unwrap_or(created_on);

        Ok(mt::UserDefinedFunction {
            name: p.name,
            description: p.description,
            definition: udf_def,
            created_on,
            update_on,
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
            mt::UDFDefinition::UDFScript(udf_script) => {
                pb::user_defined_function::Definition::UdfScript(udf_script.to_pb()?)
            }
            mt::UDFDefinition::UDAFScript(udaf_script) => {
                pb::user_defined_function::Definition::UdafScript(udaf_script.to_pb()?)
            }
            mt::UDFDefinition::UDTF(udtf) => {
                pb::user_defined_function::Definition::Udtf(udtf.to_pb()?)
            }
            mt::UDFDefinition::ScalarUDF(scalar_udf) => {
                pb::user_defined_function::Definition::ScalarUdf(scalar_udf.to_pb()?)
            }
            mt::UDFDefinition::UDTFServer(udtf_server) => {
                pb::user_defined_function::Definition::UdtfServer(udtf_server.to_pb()?)
            }
        };

        Ok(pb::UserDefinedFunction {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            name: self.name.clone(),
            description: self.description.clone(),
            definition: Some(udf_def),
            created_on: Some(self.created_on.to_pb()?),
            update_on: Some(self.update_on.to_pb()?),
        })
    }
}
