---
title: Backup and Restore Databend Meta Service
---

This guideline will introduce how to backup and restore the databend-meta datas.

## Export data from metasrv

Shut down the metasrv.

Then export sled DB from the dir(`<your_meta_dir>`) in which the metasrv
stores meta to a local file `output_fn`, in multi-line JSON format.
E.g., every line in the output file is a JSON of an exported key-value record.

```sh
# cargo build --bin databend-metactl

./target/debug/databend-metactl --export --raft-dir "<your_meta_dir>" > "<output_fn>"

# tail "<output_fn>"
# ["state_machine/0",{"Nodes":{"key":2,"value":{"name":"","endpoint":{"addr":"localhost","port":28203}}}}]
# ["state_machine/0",{"Nodes":{"key":3,"value":{"name":"","endpoint":{"addr":"localhost","port":28303}}}}]
# ["state_machine/0",{"StateMachineMeta":{"key":"LastApplied","value":{"LogId":{"term":1,"index":378}}}}]
# ["state_machine/0",{"StateMachineMeta":{"key":"Initialized","value":{"Bool":true}}}]
# ...
```

##  Restore a metasrv

The following command rebuild a metasrv db in `<your_meta_dir>` from
exported meta data:

```sh
cat "<output_fn>" | ./target/debug/databend-metactl --import --raft-dir "<your_meta_dir>"

databend-meta --raft-dir "<your_meta_dir>" ...
```

**Caveat**: Data in `<your_meta_dir>` will be cleared.
