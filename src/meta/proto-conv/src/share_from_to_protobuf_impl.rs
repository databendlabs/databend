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

use std::collections::BTreeSet;

use chrono::DateTime;
use chrono::Utc;
use databend_common_meta_app::share as mt;
use databend_common_protos::pb;

use crate::reader_check_msg;
use crate::FromToProto;
use crate::Incompatible;
use crate::MIN_READER_VER;
use crate::VER;

impl FromToProto for mt::ObjectSharedByShareIds {
    type PB = pb::ObjectSharedByShareIds;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::ObjectSharedByShareIds) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let v = Self {
            share_ids: BTreeSet::from_iter(p.share_ids.iter().copied()),
        };
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::ObjectSharedByShareIds, Incompatible> {
        let p = pb::ObjectSharedByShareIds {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            share_ids: Vec::from_iter(self.share_ids.iter().copied()),
        };
        Ok(p)
    }
}

impl FromToProto for mt::ShareAccountMeta {
    type PB = pb::ShareAccountMeta;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::ShareAccountMeta) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        Ok(mt::ShareAccountMeta {
            account: p.account.clone(),
            share_id: p.share_id,
            share_on: DateTime::<Utc>::from_pb(p.share_on)?,
            accept_on: match p.accept_on {
                Some(t) => Some(DateTime::<Utc>::from_pb(t)?),
                None => None,
            },
        })
    }

    fn to_pb(&self) -> Result<pb::ShareAccountMeta, Incompatible> {
        Ok(pb::ShareAccountMeta {
            ver: VER,
            min_reader_ver: MIN_READER_VER,

            account: self.account.clone(),
            share_id: self.share_id,
            share_on: self.share_on.to_pb()?,
            accept_on: match &self.accept_on {
                Some(t) => Some(t.to_pb()?),
                None => None,
            },
        })
    }
}

impl FromToProto for mt::ShareEndpointMeta {
    type PB = pb::ShareEndpointMeta;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::ShareEndpointMeta) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        Ok(mt::ShareEndpointMeta {
            url: p.url.clone(),
            tenant: p.tenant.clone(),
            args: p.args.clone(),
            comment: p.comment.clone(),
            create_on: DateTime::<Utc>::from_pb(p.create_on)?,
            credential: if let Some(credential) = p.credential {
                Some(mt::ShareCredential::from_pb(credential)?)
            } else {
                None
            },
        })
    }

    fn to_pb(&self) -> Result<pb::ShareEndpointMeta, Incompatible> {
        Ok(pb::ShareEndpointMeta {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            url: self.url.clone(),
            tenant: self.tenant.clone(),
            args: self.args.clone(),
            comment: self.comment.clone(),
            create_on: self.create_on.to_pb()?,
            credential: if let Some(credential) = &self.credential {
                Some(credential.to_pb()?)
            } else {
                None
            },
        })
    }
}

impl FromToProto for mt::ShareCredential {
    type PB = pb::ShareCredential;
    fn get_pb_ver(_p: &Self::PB) -> u64 {
        0
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        match p.credential {
            Some(pb::share_credential::Credential::Hmac(hmac)) => {
                Ok(mt::ShareCredential::HMAC(mt::ShareCredentialHmac {
                    key: hmac.key.clone(),
                }))
            }
            None => Err(Incompatible {
                reason: "ShareCredential cannot be None".to_string(),
            }),
        }
    }

    fn to_pb(&self) -> Result<Self::PB, Incompatible> {
        match self {
            Self::HMAC(hmac) => Ok(Self::PB {
                credential: Some(pb::share_credential::Credential::Hmac(
                    pb::ShareCredentialHmac {
                        ver: VER,
                        min_reader_ver: MIN_READER_VER,
                        key: hmac.key.clone(),
                    },
                )),
            }),
        }
    }
}
