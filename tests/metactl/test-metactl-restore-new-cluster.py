#!/usr/bin/env python3

import json
import shutil
import time

from metactl_utils import metactl_export, metactl_import, cluster_status
from utils import kill_databend_meta, start_meta_node, print_title, print_step


def main():
    kill_databend_meta()

    shutil.rmtree(".databend", ignore_errors=True)

    print_title("1. Start 3 meta node cluster to generate raft logs")

    start_meta_node(1, False)
    start_meta_node(2, False)
    start_meta_node(3, False)

    old_status = cluster_status("old cluster")
    print(json.dumps(old_status, indent=2))

    kill_databend_meta()

    print_title("2. Export meta node data")

    metactl_export("./.databend/meta1", "meta.db")

    exported = metactl_export("./.databend/meta1", None)
    print_step("Exported meta data. start")
    print(exported)
    print_step("Exported meta data. end")

    print_step("Clear meta service data dirs")
    shutil.rmtree(".databend")

    print_title("3. Import old meta node data to new cluster")

    cluster = {
        4: "localhost:29103",
        5: "localhost:29203",
        6: "localhost:29303",
    }

    metactl_import("./.databend/new_meta1", 4, "meta.db", cluster)
    metactl_import("./.databend/new_meta2", 5, "meta.db", cluster)
    metactl_import("./.databend/new_meta3", 6, "meta.db", cluster)

    print_step(
        "3.1 Check if state machine is complete by checking key 'LastMembership'"
    )

    meta1_data = metactl_export("./.databend/new_meta1", None)
    print(meta1_data)

    meta2_data = metactl_export("./.databend/new_meta2", None)
    print(meta2_data)

    meta3_data = metactl_export("./.databend/new_meta3", None)
    print(meta3_data)

    assert "LastMembership" in meta1_data
    assert "LastMembership" in meta2_data
    assert "LastMembership" in meta3_data

    print_title("3.2 Start 3 new meta node cluster")

    start_meta_node(1, True)
    start_meta_node(2, True)
    start_meta_node(3, True)

    print_step("sleep 5 sec to wait for membership to commit")
    time.sleep(5)

    print_title("3.3 Check membership in new cluster")

    new_status = cluster_status("new cluster")
    print(json.dumps(new_status, indent=2))

    voters = new_status["voters"]

    names = dict([(voter["name"], voter) for voter in voters])

    assert names["4"]["endpoint"] == {"addr": "localhost", "port": 29103}
    assert names["5"]["endpoint"] == {"addr": "localhost", "port": 29203}
    assert names["6"]["endpoint"] == {"addr": "localhost", "port": 29303}

    kill_databend_meta()
    shutil.rmtree(".databend")

    print_title("4. Import with --initial-cluster of one node")

    cluster = {
        # This will be replaced when meta node starts
        4: "127.0.0.1:12345",
    }

    metactl_import("./.databend/new_meta1", 4, "meta.db", cluster)

    print_step(
        "4.1 Check if state machine is complete by checking key 'LastMembership'"
    )
    meta1_data = metactl_export("./.databend/new_meta1", None)
    print(meta1_data)
    assert "LastMembership" in meta1_data

    print_title("4.2 Start new meta node cluster")

    start_meta_node(1, True)

    print_step("sleep 3 sec to wait for membership to commit")
    time.sleep(3)

    print_title("4.3 Check membership in new cluster")

    new_status = cluster_status("new single node cluster")
    print(json.dumps(new_status, indent=2))

    voters = new_status["voters"]

    names = dict([(voter["name"], voter) for voter in voters])

    # The address is replaced with the content in config.
    assert names["4"]["endpoint"] == {"addr": "localhost", "port": 29103}

    kill_databend_meta()
    shutil.rmtree(".databend")


if __name__ == "__main__":
    main()
