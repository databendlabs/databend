# Tests

## Stateless Tests

Stateless Test is functional test.

It uses MySQL client and get the result for checking.

### How to Run

#### 1. Start fuse-query server
`make run`

#### 2. Run fuse-test
```
cd tests/
./fuse-test
```

If a test fails, there are two files:
* *.stdout is the actual result
* *.stderr is the stderr output


