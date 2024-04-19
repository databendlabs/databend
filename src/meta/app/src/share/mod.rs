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

#[allow(clippy::module_inception)]
mod share;

pub mod share_consumer_ident;
pub mod share_end_point_ident;
pub mod share_name_ident;

pub use share::AddShareAccountsReply;
pub use share::AddShareAccountsReq;
pub use share::CreateShareEndpointReply;
pub use share::CreateShareEndpointReq;
pub use share::CreateShareReply;
pub use share::CreateShareReq;
pub use share::DropShareEndpointReply;
pub use share::DropShareEndpointReq;
pub use share::DropShareReply;
pub use share::DropShareReq;
pub use share::GetObjectGrantPrivilegesReply;
pub use share::GetObjectGrantPrivilegesReq;
pub use share::GetShareEndpointReply;
pub use share::GetShareEndpointReq;
pub use share::GetShareGrantObjectReply;
pub use share::GetShareGrantObjectReq;
pub use share::GetShareGrantTenants;
pub use share::GetShareGrantTenantsReply;
pub use share::GetShareGrantTenantsReq;
pub use share::GrantShareObjectReply;
pub use share::GrantShareObjectReq;
pub use share::ObjectGrantPrivilege;
pub use share::ObjectSharedByShareIds;
pub use share::RemoveShareAccountsReply;
pub use share::RemoveShareAccountsReq;
pub use share::RevokeShareObjectReply;
pub use share::RevokeShareObjectReq;
pub use share::ShareAccountMeta;
pub use share::ShareAccountReply;
pub use share::ShareDatabaseSpec;
pub use share::ShareEndpointId;
pub use share::ShareEndpointIdToName;
pub use share::ShareEndpointMeta;
pub use share::ShareGrantEntry;
pub use share::ShareGrantObject;
pub use share::ShareGrantObjectName;
pub use share::ShareGrantObjectPrivilege;
pub use share::ShareGrantObjectSeqAndId;
pub use share::ShareGrantReplyObject;
pub use share::ShareId;
pub use share::ShareIdToName;
pub use share::ShareIdent;
pub use share::ShareInfo;
pub use share::ShareMeta;
pub use share::ShareSpec;
pub use share::ShareTableInfoMap;
pub use share::ShareTableSpec;
pub use share::ShowSharesReply;
pub use share::ShowSharesReq;
pub use share::TableInfoMap;
pub use share::UpsertShareEndpointReply;
pub use share::UpsertShareEndpointReq;
pub use share_consumer_ident::ShareConsumerIdent;
pub use share_end_point_ident::ShareEndpointIdent;
