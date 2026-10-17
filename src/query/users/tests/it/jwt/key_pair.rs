// Copyright 2026 Datafuse Labs.
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

use databend_common_meta_app::principal::PublicKeyEntry;
use databend_common_meta_app::principal::normalize_public_key;
use databend_common_users::verify_key_pair_jwt;
use jwt_simple::prelude::*;

fn public_key_entry(public_key_pem: &str) -> anyhow::Result<PublicKeyEntry> {
    Ok(PublicKeyEntry {
        key: normalize_public_key(public_key_pem)?,
        label: "test-key".to_string(),
        created_at: 0,
    })
}

#[test]
fn test_key_pair_jwt_time_tolerance() -> anyhow::Result<()> {
    let key_pair = RS256KeyPair::generate(2048)?;
    let public_key = public_key_entry(&key_pair.public_key().to_pem()?)?;
    let public_keys = [public_key];

    let now = Clock::now_since_epoch();

    let mut tolerated_claims = Claims::create(Duration::from_hours(2));
    tolerated_claims.issued_at = Some(now + Duration::from_secs(4));
    let tolerated_token = key_pair.sign(tolerated_claims)?;
    verify_key_pair_jwt(&tolerated_token, &public_keys)?;

    let mut rejected_claims = Claims::create(Duration::from_hours(2));
    rejected_claims.issued_at = Some(now + Duration::from_secs(30));
    let rejected_token = key_pair.sign(rejected_claims)?;
    assert!(verify_key_pair_jwt(&rejected_token, &public_keys).is_err());

    Ok(())
}
