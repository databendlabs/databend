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

use mini_redis::Connection;
use mini_redis::Frame;
use tokio::net::TcpListener;
use tokio::net::TcpStream;

// A mock Redis server used as dictionary source to test.
pub async fn run_redis_source() {
    let listener = TcpListener::bind("0.0.0.0:6379").await.unwrap();

    loop {
        let (socket, _) = listener.accept().await.unwrap();

        // A new task is spawned for each inbound socket.  The socket is
        // moved to the new task and processed there.
        databend_common_base::runtime::spawn(async move {
            process(socket).await;
        });
    }
}

async fn process(socket: TcpStream) {
    use std::collections::HashMap;

    use mini_redis::Command::Get;
    use mini_redis::Command::{self};

    // A mock db is used to store test keys and values.
    let mut db = HashMap::new();
    db.insert("a".to_string(), "abc".as_bytes().to_vec());
    db.insert("b".to_string(), "def".as_bytes().to_vec());

    // Connection, provided by `mini-redis`, handles parsing frames from
    // the socket
    let mut connection = Connection::new(socket);

    // Use `read_frame` to receive a command from the connection.
    while let Some(frame) = connection.read_frame().await.unwrap() {
        let response = match Command::from_frame(frame).unwrap() {
            Get(cmd) => {
                // Return a value if the first character of the key is ASCII alphanumeric,
                // otherwise treat it as the key does not exist.
                if cmd.key().starts_with(|c: char| c.is_ascii_alphanumeric()) {
                    let value = format!("{}_value", cmd.key());
                    Frame::Bulk(value.into())
                } else {
                    Frame::Null
                }
            }
            _ => Frame::Simple("Ok".to_string()),
        };

        // Write the response to the client
        connection.write_frame(&response).await.unwrap();
    }
}
