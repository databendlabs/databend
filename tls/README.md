# Enable TLS verification in datafuse
TLS bring security enhancement toward datafuse cluster, and should be supported by default
## Example on how to setup TLS for fuse-query server
1. Generate certificate private key and cert
```bash
make generate
```
Remember to set up common name as host name(here is 127.0.0.1)
```bash
Country Name (2 letter code) []:
State or Province Name (full name) []:
Locality Name (eg, city) []:
Organization Name (eg, company) []: 
Organizational Unit Name (eg, section) []:
Common Name (eg, fully qualified host name) []:127.0.0.1
Email Address []:
```
2. start fuse-query server with tls enabled
```bash
./target/release/fuse-query --tls-server-cert ./tls/certs/public.crt  --tls-server-key  ./tls/certs/private.key
```
3. test self-signed certificate through curl
Generate pem chain
```bash
echo quit | openssl s_client -showcerts -servername localhost -connect 127.0.0.1:8080  > cacert.pem
```   
Provide pem chain for curl
```bash
curl --cacert cacert.pem https://127.0.0.1:8080/v1/hello 
```
you should see it works
```bash
Config { log_level: "INFO", log_dir: "./_logs", num_cpus: 8, mysql_handler_host: "127.0.0.1", mysql_handler_port: 3307, max_active_sessions: 256, clickhouse_handler_host: "127.0.0.1", clickhouse_handler_port: 9000, flight_api_address: "127.0.0.1:9090", http_api_address: "127.0.0.1:8080", metric_api_address: "127.0.0.1:7070", store_api_address: "127.0.0.1:9191", store_api_username: ******, store_api_password: ******, config_file: "", tls_server_cert: "./tls/certs/public.crt", tls_server_key: "./tls/certs/private.key" }
```
if we do not provide pem chain
```bash
curl https://127.0.0.1:8080/v1/hello
```
error message should occur
```bash
curl: (60) SSL certificate problem: self signed certificate
More details here: https://curl.haxx.se/docs/sslcerts.html

curl failed to verify the legitimacy of the server and therefore could not
establish a secure connection to it. To learn more about this situation and
how to fix it, please visit the web page mentioned above.
```