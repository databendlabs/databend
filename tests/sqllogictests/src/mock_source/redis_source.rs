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

use std::collections::VecDeque;

use tokio::net::TcpListener;
use tokio::net::TcpStream;

pub async fn run_redis_source() {
    // Bind the listener to the address
    let listener = TcpListener::bind("0.0.0.0:6379").await.unwrap();

    loop {
        let (socket, _) = listener.accept().await.unwrap();

        // A new task is spawned for each inbound socket. The socket is
        // moved to the new task and processed there.
        databend_common_base::runtime::spawn(async move {
            process(socket).await;
        });
    }
}

async fn process(stream: TcpStream) {
    let mut buf = Vec::with_capacity(4096);
    loop {
        buf.clear();
        // Wait for the socket to be readable
        stream.readable().await.unwrap();

        let mut ret_values = VecDeque::new();
        match stream.try_read_buf(&mut buf) {
            Ok(0) => break,
            Ok(_) => {
                let request = String::from_utf8(buf.clone()).unwrap();
                let cmds = parse_resp(request);
                for cmd in cmds {
                    if let Command::Get(key) = cmd {
                        // Return a value if the first character of the key is ASCII alphanumeric,
                        // otherwise treat it as the key does not exist.
                        let ret_value = if key.starts_with(|c: char| c.is_ascii_alphanumeric()) {
                            let v = format!("{}_value", key);
                            format!("${}\r\n{}\r\n", v.len(), v)
                        } else {
                            "$-1\r\n".to_string()
                        };
                        ret_values.push_back(ret_value);
                    } else {
                        let ret_value = "+OK\r\n".to_string();
                        ret_values.push_back(ret_value);
                    }
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                continue;
            }
            Err(_) => {
                let ret_value = "+OK\r\n".to_string();
                ret_values.push_back(ret_value);
            }
        }

        while let Some(ret_value) = ret_values.pop_front() {
            // Wait for the socket to be writable
            stream.writable().await.unwrap();

            match stream.try_write(ret_value.as_bytes()) {
                Ok(_) => {}
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    continue;
                }
                Err(_) => {
                    break;
                }
            }
        }
    }
}

// Redis command, only support get, other commands are ignored.
enum Command {
    Get(String),
    Invalid,
    Other,
}

// parse RESP(REdis Serialization Protocol)
// for example: "*2\r\n$3\r\nGET\r\n$2\r\nabc\r\n"
fn parse_resp(request: String) -> Vec<Command> {
    // split by \r\n
    let mut lines = request.split("\r\n").collect::<Vec<_>>();
    let mut cmds = Vec::new();
    while !lines.is_empty() {
        if lines[0].is_empty() {
            break;
        }
        let len: usize = lines[0][1..].parse().unwrap();
        let n = 2 * len + 1;
        if lines.len() < n {
            cmds.push(Command::Invalid);
            return cmds;
        }
        // only parse GET command and ingore other commands
        if lines[2] == "GET" {
            let cmd = Command::Get(lines[4].to_string());
            cmds.push(cmd);
        } else {
            cmds.push(Command::Other);
        }
        lines.drain(0..n);
    }
    cmds
}
