// Copyright 2022 Datafuse Labs.
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

use std::fmt::Debug;
use std::fmt::Display;

use convert_case::Casing;
use databend_common_proto_conv::FromToProto;
use databend_common_proto_conv::VER;
use pretty_assertions::assert_eq;

/// Tests converting rust types from/to protobuf defined types.
/// It also print out encoded protobuf message as data for backward compatibility test.
pub(crate) fn test_pb_from_to<MT>(name: impl Display, m: MT) -> anyhow::Result<()>
where MT: FromToProto + PartialEq + Debug {
    let p = m.to_pb()?;

    let n = std::any::type_name::<MT>();

    let mut buf = vec![];
    prost::Message::encode(&p, &mut buf)?;
    println!("Encoded buffer: {:?}", buf);

    let var_name = n.split("::").last().unwrap();
    // The encoded data should be saved for compatability test.
    println!("// Encoded data of version {} of {}:", VER, n);
    println!("// It is generated with common::test_pb_from_to().");
    println!(
        "let {}_v{} = vec!{:?};",
        var_name.to_case(convert_case::Case::Snake),
        VER,
        buf
    );

    let got = MT::from_pb(p)?;
    assert_eq!(m, got, "convert from/to protobuf: {}", name);
    Ok(())
}

/// Tests loading old version data.
///
/// Returns the message version.
pub(crate) fn test_load_old<MT>(
    name: impl Display,
    buf: &[u8],
    want_msg_ver: u64,
    want: MT,
) -> anyhow::Result<()>
where
    MT: FromToProto + PartialEq + Debug,
{
    let p: MT::PB = prost::Message::decode(buf).map_err(print_err)?;
    assert_eq!(want_msg_ver, MT::get_pb_ver(&p), "loading {}", name);

    let got = MT::from_pb(p).map_err(print_err)?;

    assert_eq!(want, got, "loading {} with version {} program", name, VER);
    Ok(())
}

pub(crate) fn print_err<T: Debug>(e: T) -> T {
    eprintln!("Error: {:?}", e);
    e
}
