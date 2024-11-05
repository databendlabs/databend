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

//! Test metasrv SchemaApi by writing to one node and then reading from another,
//! on a restarted cluster.

use std::ops::Deref;
use std::time::Duration;

use databend_common_grpc::ConnectionFactory;
use databend_common_meta_client::from_digit_ver;
use databend_common_meta_client::to_digit_ver;
use databend_common_meta_client::MetaChannelManager;
use databend_common_meta_client::MetaGrpcClient;
use databend_common_meta_client::METACLI_COMMIT_SEMVER;
use databend_common_meta_client::MIN_METASRV_SEMVER;
use databend_meta::version::MIN_METACLI_SEMVER;
use log::debug;
use log::info;
use semver::Version;
use test_harness::test;

use crate::testing::meta_service_test_harness;
use crate::tests::start_metasrv;

/// - Test client version < serverside min-compatible-client-ver.
/// - Test metasrv version < client min-compatible-metasrv-ver.
#[test(harness = meta_service_test_harness)]
#[fastrace::trace]
async fn test_metasrv_handshake() -> anyhow::Result<()> {
    fn smaller_ver(v: &Version) -> Version {
        if v.major > 0 {
            Version::new(v.major - 1, v.minor, v.patch)
        } else if v.minor > 0 {
            Version::new(0, v.minor - 1, v.patch)
        } else if v.patch > 0 {
            Version::new(0, 0, v.patch - 1)
        } else {
            unreachable!("can not build a semver smaller than {:?}", v)
        }
    }

    let (_tc, addr) = start_metasrv().await?;

    let c = ConnectionFactory::create_rpc_channel(addr, Some(Duration::from_millis(1000)), None)
        .await?;
    let (mut client, _once) = MetaChannelManager::new_real_client(c);

    info!("--- client has smaller ver than S.min_cli_ver");
    {
        let min_client_ver = &MIN_METACLI_SEMVER;
        let cli_ver = smaller_ver(min_client_ver);

        let res =
            MetaGrpcClient::handshake(&mut client, &cli_ver, &MIN_METASRV_SEMVER, "root", "xxx")
                .await;

        debug!("handshake res: {:?}", res);
        let e = res.unwrap_err();

        let want = format!(
            "meta-client protocol_version({}) < metasrv min-compatible({})",
            cli_ver, MIN_METACLI_SEMVER
        );
        assert!(e.to_string().contains(&want), "handshake err: {:?}", e);
    }

    info!("--- server has smaller ver than C.min_srv_ver");
    {
        let min_srv_ver = &MIN_METASRV_SEMVER;
        let mut min_srv_ver = min_srv_ver.clone();
        min_srv_ver.major += 1;

        let res = MetaGrpcClient::handshake(
            &mut client,
            &METACLI_COMMIT_SEMVER,
            &min_srv_ver,
            "root",
            "xxx",
        )
        .await;

        debug!("handshake res: {:?}", res);
        let e = res.unwrap_err();

        let want = format!(
            "metasrv protocol_version({}) < meta-client min-compatible({})",
            // strip `nightly` from 0.7.57-nightly
            from_digit_ver(to_digit_ver(METACLI_COMMIT_SEMVER.deref(),)),
            min_srv_ver,
        );
        assert!(
            e.to_string().contains(&want),
            "handshake err: {:?} contains: {}",
            e,
            want
        );
    }

    info!("--- old client using ver==0 is allowed");
    {
        let zero = Version::new(0, 0, 0);

        let res =
            MetaGrpcClient::handshake(&mut client, &zero, &MIN_METASRV_SEMVER, "root", "xxx").await;

        debug!("handshake res: {:?}", res);
        assert!(res.is_ok());
    }

    Ok(())
}
