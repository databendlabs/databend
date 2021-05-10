# Tests

## How to Run
First, we start fuse-query server:
`make run`

### 0. Stateless Tests


```
cd tests/
./fuse-test --skip-dir '1_performance'
```

If a test fails, there are two files:
* *.stdout is the actual result
* *.stderr is the stderr output

### 1. Performance Tests

```
cd tests/
./fuse-test --run-dir '1_performance'
```