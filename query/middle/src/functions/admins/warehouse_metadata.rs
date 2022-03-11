//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::DataSchema;
use common_exception::Result;
use common_meta_types::WarehouseInfo;

use crate::functions::Function;
use crate::functions::FunctionDescription;
use crate::functions::FunctionFeatures;
use crate::sessions::QueryContext;
use crate::storages::system::WarehousesTable;

pub struct CreateWarehouseMetaFunction {}
pub struct UpdateWarehouseSizeFunction {}
pub struct GetWarehouseMetaFunction {}
pub struct ListWarehouseMetaFunction {}
pub struct DropWarehouseMetaFunction {}

/// Create a new warehouse with metadata.
/// examples:
/// ```rust
/// /// it would regist a new warehouse with name "warehouse*!)#@!!#@!" and size "Small" and capacity "10"
/// /// and would provide a new warehouse id for it.
/// it would return created warehouse id in warehouse info if succeeded, otherwise would return error
/// call admin$create_warehouse_meta("tenant1", "warehouse*!)#@!!#@!", "Small", "10")
/// ```
impl CreateWarehouseMetaFunction {
    pub fn try_create() -> Result<Box<dyn Function>> {
        Ok(Box::new(CreateWarehouseMetaFunction {}))
    }

    // arguments:
    // tenant_id: string
    // warehouse_name: string
    // size: string
    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().num_arguments(3))
    }
}

#[async_trait::async_trait]
impl Function for CreateWarehouseMetaFunction {
    async fn eval(&self, ctx: Arc<QueryContext>, args: Vec<String>) -> Result<DataBlock> {
        let tenant_id = args[0].clone();
        let warehouse_name = args[1].clone();
        let warehouse_size = args[2].clone();

        // regist a warehouse
        let warehouse_info = WarehouseInfo::new(
            warehouse_name.clone(),
            warehouse_name.clone(),
            warehouse_size,
        );
        let provider = ctx.get_user_manager();
        provider
            .create_warehouse(tenant_id.as_str(), warehouse_info.clone(), true)
            .await?;
        let infos = vec![warehouse_info];
        WarehousesTable::warehouse_infos_to_datablock(infos)
    }

    fn schema(&self) -> Arc<DataSchema> {
        WarehousesTable::schema()
    }
}

/// Scale an existed warehouse with metadata.
/// exmaples:
/// ```rust
/// /// this command would update the size to Small e for given warehouse under tenant
/// call admin$update_warehouse_neta_size("tenant1", "warehouse*!)#@!!#@!", "Small")
/// ```
impl UpdateWarehouseSizeFunction {
    pub fn try_create() -> Result<Box<dyn Function>> {
        Ok(Box::new(UpdateWarehouseSizeFunction {}))
    }

    // arguments:
    // tenant_id: string
    // warehouse_name: string
    // size: string
    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().num_arguments(3))
    }
}

#[async_trait::async_trait]
impl Function for UpdateWarehouseSizeFunction {
    async fn eval(&self, ctx: Arc<QueryContext>, args: Vec<String>) -> Result<DataBlock> {
        let tenant_id = args[0].clone();
        let warehouse_name = args[1].clone();
        let size = args[2].clone();

        // regist a warehouse
        let provider = ctx.get_user_manager();
        provider
            .update_warehouse_size(tenant_id.as_str(), warehouse_name.as_str(), size.as_str())
            .await?;
        let info = provider
            .get_warehouse(tenant_id.as_str(), warehouse_name.as_str())
            .await?;
        let infos = vec![info];
        WarehousesTable::warehouse_infos_to_datablock(infos)
    }

    fn schema(&self) -> Arc<DataSchema> {
        WarehousesTable::schema()
    }
}

/// Get the metadata for a registed warehouse
/// exmaples:
/// ```rust
/// /// this command would return the warehouse info contain warehouse id for given warehouse name.
/// /// it would return error if the warehouse is not registed.
/// call admin$get_warehouse_meta("tenant1", "warehouse*!)#@!!#@!")
/// ```
impl GetWarehouseMetaFunction {
    pub fn try_create() -> Result<Box<dyn Function>> {
        Ok(Box::new(GetWarehouseMetaFunction {}))
    }

    // arguments:
    // tenant_id: string
    // warehouse_name: string
    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().num_arguments(2))
    }
}

#[async_trait::async_trait]
impl Function for GetWarehouseMetaFunction {
    async fn eval(&self, ctx: Arc<QueryContext>, args: Vec<String>) -> Result<DataBlock> {
        let tenant_id = args[0].clone();
        let warehouse_name = args[1].clone();

        let provider = ctx.get_user_manager();
        let info = provider
            .get_warehouse(tenant_id.as_str(), warehouse_name.as_str())
            .await?;
        let infos = vec![info];
        WarehousesTable::warehouse_infos_to_datablock(infos)
    }

    fn schema(&self) -> Arc<DataSchema> {
        WarehousesTable::schema()
    }
}

/// List all registed warehouses under tenant
/// exmaples:
/// ```rust
/// /// this command would return all warehouse info under a tenant.
/// call admin$list_warehouse_meta("tenant1")
/// ```
impl ListWarehouseMetaFunction {
    pub fn try_create() -> Result<Box<dyn Function>> {
        Ok(Box::new(ListWarehouseMetaFunction {}))
    }

    // arguments:
    // tenant_id: string
    // warehouse_name: string
    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().num_arguments(1))
    }
}

#[async_trait::async_trait]
impl Function for ListWarehouseMetaFunction {
    async fn eval(&self, ctx: Arc<QueryContext>, args: Vec<String>) -> Result<DataBlock> {
        let tenant_id = args[0].clone();
        let provider = ctx.get_user_manager();
        let infos = provider.get_warehouses(tenant_id.as_str()).await?;
        WarehousesTable::warehouse_infos_to_datablock(infos)
    }

    fn schema(&self) -> Arc<DataSchema> {
        WarehousesTable::schema()
    }
}

/// Drop a warehouse from tenant, if the warehouse is not registed, it would return ok.
/// exmaples:
/// ```rust
/// /// this command would drop the warehouse with given name under tenant
/// call admin$drop_warehouse_meta("tenant1", "warehouse*!)#@!!#@!")
/// ```
impl DropWarehouseMetaFunction {
    pub fn try_create() -> Result<Box<dyn Function>> {
        Ok(Box::new(DropWarehouseMetaFunction {}))
    }

    // arguments:
    // tenant_id: string
    // warehouse_name: string
    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().num_arguments(2))
    }
}

#[async_trait::async_trait]
impl Function for DropWarehouseMetaFunction {
    async fn eval(&self, ctx: Arc<QueryContext>, args: Vec<String>) -> Result<DataBlock> {
        let tenant_id = args[0].clone();
        let warehouse_name = args[1].clone();
        let provider = ctx.get_user_manager();
        provider
            .drop_warehouse(tenant_id.as_str(), warehouse_name.as_str(), true)
            .await?;
        Ok(DataBlock::empty())
    }

    fn schema(&self) -> Arc<DataSchema> {
        WarehousesTable::schema()
    }
}
