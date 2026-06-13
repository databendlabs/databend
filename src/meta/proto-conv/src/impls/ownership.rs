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

//! This mod is the key point about compatibility.
//! Everytime update anything in this file, update the `VER` and let the tests pass.

use databend_common_meta_app as mt;
use databend_common_protos::pb;

use crate::FromToProto;
use crate::Incompatible;
use crate::MIN_READER_VER;
use crate::VER;
use crate::reader_check_msg;

impl FromToProto for mt::principal::OwnershipInfo {
    type PB = pb::OwnershipInfo;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::OwnershipInfo) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        Ok(mt::principal::OwnershipInfo {
            role: p.role.clone(),
            object: mt::principal::OwnershipObject::from_pb(p.object.ok_or_else(|| {
                Incompatible::new(format!("ROLE {}: Object can not be None", &p.role))
            })?)?,
        })
    }

    fn to_pb(&self) -> Result<pb::OwnershipInfo, Incompatible> {
        Ok(pb::OwnershipInfo {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            role: self.role.clone(),
            object: Some(self.object.to_pb()?),
        })
    }
}

impl FromToProto for mt::principal::OwnershipObject {
    type PB = pb::OwnershipObject;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::OwnershipObject) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let Some(object) = p.object else {
            return Err(Incompatible::new(
                "OwnershipObject cannot be None".to_string(),
            ));
        };

        match object {
            pb::ownership_object::Object::Database(
                pb::ownership_object::OwnershipDatabaseObject { catalog, db },
            ) => Ok(mt::principal::OwnershipObject::Database {
                catalog_name: catalog,
                db_id: db,
            }),
            pb::ownership_object::Object::Table(pb::ownership_object::OwnershipTableObject {
                catalog,
                db,
                table,
            }) => Ok(mt::principal::OwnershipObject::Table {
                catalog_name: catalog,
                db_id: db,
                table_id: table,
            }),
            pb::ownership_object::Object::Udf(pb::ownership_object::OwnershipUdfObject { udf }) => {
                Ok(mt::principal::OwnershipObject::UDF { name: udf })
            }
            pb::ownership_object::Object::Stage(pb::ownership_object::OwnershipStageObject {
                stage,
            }) => Ok(mt::principal::OwnershipObject::Stage { name: stage }),
            pb::ownership_object::Object::Warehouse(
                pb::ownership_object::OwnershipWarehouseObject { id },
            ) => Ok(mt::principal::OwnershipObject::Warehouse { id }),
            pb::ownership_object::Object::Connection(
                pb::ownership_object::OwnershipConnectionObject { connection },
            ) => Ok(mt::principal::OwnershipObject::Connection { name: connection }),
            pb::ownership_object::Object::Sequence(
                pb::ownership_object::OwnershipSequenceObject { sequence },
            ) => Ok(mt::principal::OwnershipObject::Sequence { name: sequence }),
            pb::ownership_object::Object::Procedure(
                pb::ownership_object::OwnershipProcedureObject { procedure_id },
            ) => Ok(mt::principal::OwnershipObject::Procedure { procedure_id }),
            pb::ownership_object::Object::MaskingPolicy(
                pb::ownership_object::OwnershipMaskingPolicyObject { policy_id },
            ) => Ok(mt::principal::OwnershipObject::MaskingPolicy { policy_id }),
            pb::ownership_object::Object::RowAccessPolicy(
                pb::ownership_object::OwnershipRowAccessPolicyObject { policy_id },
            ) => Ok(mt::principal::OwnershipObject::RowAccessPolicy { policy_id }),
        }
    }

    fn to_pb(&self) -> Result<pb::OwnershipObject, Incompatible> {
        let object = match self {
            mt::principal::OwnershipObject::Database {
                catalog_name,
                db_id,
            } => Some(pb::ownership_object::Object::Database(
                pb::ownership_object::OwnershipDatabaseObject {
                    catalog: catalog_name.clone(),
                    db: *db_id,
                },
            )),
            mt::principal::OwnershipObject::Table {
                catalog_name,
                db_id,
                table_id,
            } => Some(pb::ownership_object::Object::Table(
                pb::ownership_object::OwnershipTableObject {
                    catalog: catalog_name.clone(),
                    db: *db_id,
                    table: *table_id,
                },
            )),
            mt::principal::OwnershipObject::UDF { name } => {
                Some(pb::ownership_object::Object::Udf(
                    pb::ownership_object::OwnershipUdfObject { udf: name.clone() },
                ))
            }
            mt::principal::OwnershipObject::Stage { name } => Some(
                pb::ownership_object::Object::Stage(pb::ownership_object::OwnershipStageObject {
                    stage: name.clone(),
                }),
            ),
            mt::principal::OwnershipObject::Warehouse { id } => {
                Some(pb::ownership_object::Object::Warehouse(
                    pb::ownership_object::OwnershipWarehouseObject { id: id.clone() },
                ))
            }
            mt::principal::OwnershipObject::Connection { name } => {
                Some(pb::ownership_object::Object::Connection(
                    pb::ownership_object::OwnershipConnectionObject {
                        connection: name.clone(),
                    },
                ))
            }
            mt::principal::OwnershipObject::Sequence { name } => {
                Some(pb::ownership_object::Object::Sequence(
                    pb::ownership_object::OwnershipSequenceObject {
                        sequence: name.clone(),
                    },
                ))
            }
            mt::principal::OwnershipObject::Procedure { procedure_id } => {
                Some(pb::ownership_object::Object::Procedure(
                    pb::ownership_object::OwnershipProcedureObject {
                        procedure_id: *procedure_id,
                    },
                ))
            }
            mt::principal::OwnershipObject::MaskingPolicy { policy_id } => {
                Some(pb::ownership_object::Object::MaskingPolicy(
                    pb::ownership_object::OwnershipMaskingPolicyObject {
                        policy_id: *policy_id,
                    },
                ))
            }
            mt::principal::OwnershipObject::RowAccessPolicy { policy_id } => {
                Some(pb::ownership_object::Object::RowAccessPolicy(
                    pb::ownership_object::OwnershipRowAccessPolicyObject {
                        policy_id: *policy_id,
                    },
                ))
            }
        };
        Ok(pb::OwnershipObject {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            object,
        })
    }
}
