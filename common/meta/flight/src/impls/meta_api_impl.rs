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
//

use std::sync::Arc;

use common_exception::ErrorCode;
use common_meta_api::MetaApi;
use common_meta_types::CreateDatabaseReply;
use common_meta_types::CreateDatabaseReq;
use common_meta_types::CreateTableReply;
use common_meta_types::CreateTableReq;
use common_meta_types::DatabaseInfo;
use common_meta_types::DropDatabaseReply;
use common_meta_types::DropDatabaseReq;
use common_meta_types::DropTableReply;
use common_meta_types::DropTableReq;
use common_meta_types::GetDatabaseReq;
use common_meta_types::GetTableReq;
use common_meta_types::ListTableReq;
use common_meta_types::MetaId;
use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use common_meta_types::UpsertTableOptionReply;
use common_meta_types::UpsertTableOptionReq;

use crate::FlightReq;
use crate::GetTableExtReq;
use crate::ListDatabasesAction;
use crate::MetaFlightClient;

#[async_trait::async_trait]
impl MetaApi for MetaFlightClient {
    async fn create_database(
        &self,
        req: CreateDatabaseReq,
    ) -> Result<CreateDatabaseReply, ErrorCode> {
        self.do_action(FlightReq { req }).await
    }

    async fn drop_database(&self, req: DropDatabaseReq) -> Result<DropDatabaseReply, ErrorCode> {
        self.do_action(FlightReq { req }).await
    }

    async fn get_database(&self, req: GetDatabaseReq) -> Result<Arc<DatabaseInfo>, ErrorCode> {
        self.do_action(FlightReq { req }).await
    }

    async fn list_databases(&self) -> common_exception::Result<Vec<Arc<DatabaseInfo>>> {
        self.do_action(ListDatabasesAction {}).await
    }

    async fn create_table(&self, req: CreateTableReq) -> Result<CreateTableReply, ErrorCode> {
        self.do_action(FlightReq { req }).await
    }

    async fn drop_table(&self, req: DropTableReq) -> Result<DropTableReply, ErrorCode> {
        self.do_action(FlightReq { req }).await
    }

    async fn get_table(&self, req: GetTableReq) -> common_exception::Result<Arc<TableInfo>> {
        self.do_action(FlightReq { req }).await
    }

    async fn list_tables(
        &self,
        req: ListTableReq,
    ) -> common_exception::Result<Vec<Arc<TableInfo>>> {
        self.do_action(FlightReq { req }).await
    }

    async fn get_table_by_id(
        &self,
        table_id: MetaId,
    ) -> common_exception::Result<(TableIdent, Arc<TableMeta>)> {
        let x = self.do_action(GetTableExtReq { tbl_id: table_id }).await?;
        Ok((x.ident, Arc::new(x.meta)))
    }

    async fn upsert_table_option(
        &self,
        req: UpsertTableOptionReq,
    ) -> Result<UpsertTableOptionReply, ErrorCode> {
        self.do_action(FlightReq { req }).await
    }

    fn name(&self) -> String {
        "MetaFlightClient".to_string()
    }
}
