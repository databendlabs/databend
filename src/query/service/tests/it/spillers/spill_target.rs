// Integration test for SpillTarget::from_storage_params semantics.

use databend_common_meta_app::storage::StorageFsConfig;
use databend_common_meta_app::storage::StorageParams;
use databend_query::spillers::SpillTarget;

#[test]
fn test_spill_target_from_storage_params() {
    // Fs backend should be treated as local spill.
    let fs_params = StorageParams::Fs(StorageFsConfig {
        root: "/tmp/test-root".to_string(),
    });
    let target = SpillTarget::from_storage_params(Some(&fs_params));
    assert!(target.is_local());

    // None (or any non-Fs backend) should be treated as remote spill.
    let target_none = SpillTarget::from_storage_params(None);
    assert!(!target_none.is_local());
}
