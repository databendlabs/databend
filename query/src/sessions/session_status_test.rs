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
//

use std::net::SocketAddr;
use std::sync::Arc;

use common_exception::Result;

use crate::clusters::Cluster;
use crate::sessions::DatabendQueryContextShared;
use crate::sessions::MutableStatus;
use crate::tests::SessionManagerBuilder;

#[test]
fn test_session_status() -> Result<()> {
    let mutable_status = MutableStatus::try_create()?;

    // Abort status.
    {
        mutable_status.set_abort(true);
        let val = mutable_status.get_abort();
        assert!(val);
    }

    // Current database status.
    {
        mutable_status.set_current_database("bend".to_string());
        let val = mutable_status.get_current_database();
        assert_eq!("bend", val);
    }

    // Settings.
    {
        let val = mutable_status.get_settings();
        assert!(val.get_max_threads()? > 0);
    }

    // Client host.
    {
        let demo = "127.0.0.1:80";
        let server: SocketAddr = demo.parse().unwrap();
        mutable_status.set_client_host(Some(server));

        let val = mutable_status.get_client_host();
        assert_eq!(Some(server), val);
    }

    // Current user.
    {
        mutable_status.set_current_user("user1".to_string());

        let val = mutable_status.get_current_user();
        assert_eq!(Some("user1".to_string()), val);
    }

    // io shutdown tx.
    {
        let (tx, _) = futures::channel::oneshot::channel();
        mutable_status.set_io_shutdown_tx(Some(tx));

        let val = mutable_status.take_io_shutdown_tx();
        assert!(val.is_some());

        let val = mutable_status.take_io_shutdown_tx();
        assert!(val.is_none());
    }

    // context shared.
    {
        let sessions = SessionManagerBuilder::create().build()?;
        let dummy_session = sessions.create_session("TestSession")?;
        let shared = DatabendQueryContextShared::try_create(
            sessions.get_conf().clone(),
            Arc::new(dummy_session.as_ref().clone()),
            Cluster::empty(),
        );

        mutable_status.set_context_shared(Some(shared.clone()));
        let val = mutable_status.get_context_shared();
        assert_eq!(shared.conf, val.unwrap().conf);

        let val = mutable_status.take_context_shared();
        assert_eq!(shared.conf, val.unwrap().conf);

        let val = mutable_status.get_context_shared();
        assert!(val.is_none());
    }

    Ok(())
}
