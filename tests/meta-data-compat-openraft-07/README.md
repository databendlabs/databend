# Assert meta service is compatible with old data after upgrade openraft from 0.7 to 0.8

In this test, it import bunch of data exported by meta-service built with
openraft-0.7; And re-export it to assert it loads old format data correctly.

The data to import in this test is before `DataVersion::V0`(see below), where
`DataVersion::V0` is the version built with openraft `0.8`.

# All Data versions

- openraft 07-08 version: non-versionized. compatible with openraft-07/08;
- DataVersion::V0: the first versionized; add data header.
- DataVersion::V001: compatible with only openraft-08, compatibility is done by
    upgrading upon startup.
- DataVersion::V002: save snapshot in a disk file.

# Remove no longer supported version

To discard testing against an old version that is no longer supported,
e.g., `DataVersion::V0`:
- Download a binary built with the next version, e.g. `DataVersion::V001`,
    import the data, and export all data as the source for this test.
