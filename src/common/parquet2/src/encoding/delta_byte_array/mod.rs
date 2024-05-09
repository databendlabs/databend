// Copyright [2021] [Jorge C Leitao]
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

mod decoder;
mod encoder;

pub use decoder::Decoder;
pub use encoder::encode;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;

    #[test]
    fn basic() -> Result<(), Error> {
        let data = vec![b"Hello".as_ref(), b"Helicopter"];
        let mut buffer = vec![];
        encode(data.clone().into_iter(), &mut buffer);

        let mut decoder = Decoder::try_new(&buffer)?;
        let prefixes = decoder.by_ref().collect::<Result<Vec<_>, _>>()?;
        assert_eq!(prefixes, vec![0, 3]);

        // move to the lengths
        let mut decoder = decoder.into_lengths()?;

        let lengths = decoder.by_ref().collect::<Result<Vec<_>, _>>()?;
        assert_eq!(lengths, vec![5, 7]);

        // move to the values
        let values = decoder.values();
        assert_eq!(values, b"Helloicopter");
        Ok(())
    }
}
