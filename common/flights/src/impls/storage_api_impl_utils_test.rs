// Copyright 2020 Datafuse Labs.
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
//

#[cfg(test)]
mod test {
    use tonic::metadata::MetadataMap;

    use crate::impls::storage_api_impl_utils::get_meta;
    use crate::impls::storage_api_impl_utils::put_meta;

    #[test]
    fn test_get_set_meta() {
        let mut meta = MetadataMap::new();
        let test_db = "test_db";
        let test_tbl = "test_tbl";
        put_meta(&mut meta, test_db, test_tbl);
        let (db, tbl) = get_meta(&meta).unwrap();
        assert_eq!(test_db, db);
        assert_eq!(test_tbl, tbl);
    }
}
