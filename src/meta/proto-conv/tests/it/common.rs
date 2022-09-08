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

use common_proto_conv::FromToProto;
use common_proto_conv::VER;
use pretty_assertions::assert_eq;

/// Tests converting rust types from/to protobuf defined types.
/// It also print out encoded protobuf message as data for backward compatibility test.
pub(crate) fn test_pb_from_to<MT>(name: impl Display, m: MT) -> anyhow::Result<()>
where
    MT: FromToProto + PartialEq + Debug,
    MT::PB: common_protos::prost::Message,
{
    let p = m.to_pb()?;

    let mut buf = vec![];
    common_protos::prost::Message::encode(&p, &mut buf)?;
    // The encoded data should be saved for compatability test.
    println!("// Encoded data of version {} of {}:", VER, name);
    println!("// It is generated with common::test_pb_from_to.");
    println!("let {}_v{} = vec!{:?};", name, VER, buf);

    let got = MT::from_pb(p)?;
    assert_eq!(m, got, "convert from/to protobuf: {}", name);
    Ok(())
}

/// Tests loading old version data.
pub(crate) fn test_load_old<MT>(name: impl Display, buf: &[u8], want: MT) -> anyhow::Result<()>
where
    MT: FromToProto + PartialEq + Debug,
    MT::PB: common_protos::prost::Message + Default,
{
    let p: MT::PB = common_protos::prost::Message::decode(buf).map_err(print_err)?;
    let got = MT::from_pb(p).map_err(print_err)?;

    assert_eq!(want, got, "loading {} with version {} program", name, VER);
    Ok(())
}

pub(crate) fn print_err<T: Debug>(e: T) -> T {
    eprintln!("Error: {:?}", e);
    e
}

macro_rules! func_name {
    () => {{
        fn f() {}
        fn type_name_of<T>(_: T) -> &'static str {
            std::any::type_name::<T>()
        }
        let name = type_name_of(f);
        let n = &name[..name.len() - 3];
        let nn = n.replace("::{{closure}}", "");
        nn
    }};
}
