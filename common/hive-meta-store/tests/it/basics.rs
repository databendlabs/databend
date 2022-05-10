//  Copyright 2022 Datafuse Labs.
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

use common_hive_meta_store::TThriftHiveMetastoreSyncClient;
use common_hive_meta_store::ThriftHiveMetastoreSyncClient;
use thrift::protocol::TBinaryInputProtocol;
use thrift::protocol::TBinaryOutputProtocol;
use thrift::transport::TBufferedReadTransport;
use thrift::transport::TBufferedWriteTransport;
use thrift::transport::TIoChannel;
use thrift::transport::TTcpChannel;

#[test]
fn it_works() {
    let hms_service_address =
        std::env::var("HMS_SERVER_ADDRESS").unwrap_or("127.0.0.1:9083".to_owned());
    let mut c = TTcpChannel::new();
    c.open(hms_service_address).unwrap();
    let (i_chan, o_chan) = c.split().unwrap();
    let i_tran = TBufferedReadTransport::new(i_chan);
    let o_tran = TBufferedWriteTransport::new(o_chan);
    let i_prot = TBinaryInputProtocol::new(i_tran, true);
    let o_prot = TBinaryOutputProtocol::new(o_tran, true);
    let mut client = ThriftHiveMetastoreSyncClient::new(i_prot, o_prot);
    let db = client.get_database("default".to_string());
    assert!(db.is_ok());
}
