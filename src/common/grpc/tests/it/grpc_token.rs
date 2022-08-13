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

use common_exception::Result;
use common_grpc::GrpcClaim;
use common_grpc::GrpcToken;

#[test]
fn test_flight_token() -> Result<()> {
    let token = GrpcToken::create();
    let user = "batman";

    let claim = GrpcClaim {
        username: String::from(user),
    };

    let jwt = token.try_create_token(claim)?;
    let claim = token.try_verify_token(jwt)?;

    assert_eq!(claim.username, user);
    Ok(())
}
