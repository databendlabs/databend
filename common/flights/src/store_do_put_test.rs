// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

#[cfg(test)]
mod test {
    use tonic::metadata::MetadataMap;

    use crate::get_do_put_meta;
    use crate::set_do_put_meta;

    #[test]
    fn test_get_set_meta() {
        let mut meta = MetadataMap::new();
        let test_db = "test_db";
        let test_tbl = "test_tbl";
        set_do_put_meta(&mut meta, test_db, test_tbl);
        let (db, tbl) = get_do_put_meta(&meta).unwrap();
        assert_eq!(test_db, db);
        assert_eq!(test_tbl, tbl);
    }
}
