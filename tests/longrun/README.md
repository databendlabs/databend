# Databend Long Run Tests

## Introduction
Databend long run tests would test the data correctness and performance of Databend under concurrency and heavy load.
For example:
Test on concurrent large scale data ingestion, table maintainence(optimization, recluster and vacuum) and query.

## How to use it?
### Prerequisite
1. Prepare a databend cluster, either on local or on [cloud](https://app.databend.com).
2. Install [bendsql](https://github.com/datafuselabs/bendsql).
3. Install python3

### How to run tests?

The test would execute a serials of SQL and validation commands, and check the result.
it would first execute before test scripts (`_before.sh`), then execute concurrent test scripts repeatively, and finally execute after test scripts(`_after.sh`).
All event logs would be stored in a table on databend, which can be used for further analysis.


```lua
                      +-------------------+
                      |     Long Run      |
                      +-------------------+
                               |
                               |
                               v
                  +-----------------------+
                  |  Before Test Scripts  |
                  +-----------------------+
                               |
                               |
                               v
        +----------------------------------+
        |     Concurrent Test Scripts      |
        +----------------------------------+
        |              |                   |
        |              |                   |
        v              v                   v
+----------------+ +----------------+ +----------------+
|  Test Script 1 | |  Test Script 2 | |  Test Script 3 |
+----------------+ +----------------+ +----------------+
                               |
                               |
                               v
              +-----------------------+
              |   After Test Scripts  |
              +-----------------------+

```

To run a specific long run test case, you can use the following command:

```bash
WAREHOUSE_DSN=<target databend warehouse dsn> python3 longrun.py -d <scripts directory> -i <concurrent test iteration number>
```

For example, to run the `example` long run test case on your local databend cluster, you can use the following command:

```bash
WAREHOUSE_DSN=databend://root:@localhost:8000/?sslmode=disable python3 longrun.py -d ./example -i 3
```

The above command would run the `example` long run test case, which is located in `./example` directory, on your local databend cluster, and repeat the concurrent test scripts 3 times.

#### Parameters:

Environment variables may be used in long run test:
| Name | Description | Default Value |
| --- | --- | --- |
| WAREHOUSE_DSN | The Databend warehouse DSN | databend://root:@localhost:8000/?sslmode=disable |

Command line parameters:
| Name | Description | Default Value | Required |
| --- | --- | --- | --- |
| -d, --directory | The directory of long run test scripts | | Yes |
| -i, --iteration | The iteration number of concurrent test scripts |  | Yes |
| --logger | The target table on databend to store the test event logs | events | No |

### How to write tests?
1. Create a directory for your test case, for example `example`.
2. (Optional) Create a directory in your test case directory, for example `example/datagen`, which could be used to generate DML test data.

For example, you could generate test data on example test case by using the following command:

```bash
./example/datagen/gen.sh
```

3. Create before test scripts, which would be executed before the concurrent test scripts. For example, you could create a `example/_before.sh` file, which would be executed before the concurrent test scripts.
4. Create several concurrent test scripts, which would be executed repeatively. For example, you could `example/background.sh`, `example/dml.sh`, `example/sql.sh` , those three scripts would run concurrently. and repeat given iteration times
5. Create after test scripts to validate your results, which would be executed after the concurrent test scripts. For example, you could create a `example/_after.sh` file, which would be executed after the concurrent test scripts.