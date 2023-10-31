# databend-driver

## Build

```shell
cd bindings/nodejs
yarn install
yarn build
```

## Usage

```javascript
const { Client } = require("databend-driver");

const client = new Client(
  "databend+http://root:root@localhost:8000/?sslmode=disable",
);
const conn = await client.getConn();

await conn.exec(`CREATE TABLE test (
	i64 Int64,
	u64 UInt64,
	f64 Float64,
	s   String,
	s2  String,
	d   Date,
	t   DateTime
);`);

const rows = await conn.queryIter("SELECT * FROM test");
let row = await rows.next();
while (row) {
  console.log(row.values());
  row = await rows.next();
}
```

## Development

```shell
cd bindings/nodejs
yarn install
yarn build:debug
yarn test
```
