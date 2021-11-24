// Copyright 2021 Datafuse Labs.
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

use crate::binary::Encoder;
use crate::error_codes::UNKNOWN_EXCEPTION;
use crate::errors::Error;
use crate::protocols::*;

pub struct ExceptionResponse {}

impl ExceptionResponse {
    pub fn write(encoder: &mut Encoder, error: &Error, with_stack_trace: bool) {
        let mut code = UNKNOWN_EXCEPTION;
        let name = error.exception_name();
        let mut stack_trace = "".to_string();
        let mut message = error.to_string();

        if let Error::Server(e) = error {
            code = e.code;
            if with_stack_trace {
                stack_trace = e.stack_trace.clone();
            }
            message = e.message.clone();
        }
        encoder.uvarint(SERVER_EXCEPTION);

        encoder.write(code);
        //Name
        encoder.string(name);
        // Message
        encoder.string(message);
        // StackTrace
        encoder.string(stack_trace);
        // Nested.
        encoder.write(false);
    }
}
