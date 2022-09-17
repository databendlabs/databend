# Test Certs

We use certs inside `cfssl` for tls testing.

Please use `./gencert.sh generate_keys` to refresh all the certs.

## Current Used

The next refresh time will be `Sep 15 05:03:00 2023 GMT`

```shell
:) openssl x509 -noout -text -in ./tests/certs/tls/cfssl/server/server.pem
Certificate:
    Data:
        Version: 3 (0x2)
        Serial Number:
            6c:35:77:ed:6d:6f:49:be:80:3d:96:ce:00:d0:d2:96:21:68:ba:9c
        Signature Algorithm: sha256WithRSAEncryption
        Issuer: C = US, ST = LA, L = CA, O = Datafuselabs, OU = Databend, CN = Databend
        Validity
            Not Before: Sep 15 05:03:00 2022 GMT
            Not After : Sep 15 05:03:00 2023 GMT
        Subject: C = US, ST = CA, L = San Francisco, CN = Databend Server
        Subject Public Key Info:
            Public Key Algorithm: rsaEncryption
                RSA Public-Key: (2048 bit)
                Modulus:
                    00:af:95:7d:ac:14:90:33:47:f4:2d:6e:8b:8d:70:
                    30:d0:a6:a4:f5:67:65:87:f2:e5:8d:4b:bf:ae:4e:
                    79:00:fd:2c:24:49:ea:2d:07:18:dd:67:cd:76:c3:
                    0e:d8:f4:5f:21:26:f9:3e:db:6a:ba:3f:f3:4e:a3:
                    4e:f9:eb:d7:1c:8d:45:90:2e:bb:1d:55:67:36:ec:
                    11:86:96:f9:59:29:1e:9f:92:9b:87:95:d5:80:67:
                    a8:d5:88:21:d8:85:83:83:ce:8f:56:5d:95:0e:54:
                    dd:64:9a:72:c4:45:b1:2b:63:83:57:7e:fc:6d:1d:
                    a9:6f:ff:f2:ca:54:30:bd:09:ff:e2:2c:30:02:8a:
                    4c:f1:bf:a0:b7:2e:e7:fc:13:8f:1c:a1:ab:6e:98:
                    2f:98:cb:be:6e:9a:fb:27:4d:86:bd:ec:ab:0f:c2:
                    f9:24:10:8c:33:26:e6:08:10:02:b7:bb:de:ea:02:
                    ab:af:4f:6c:98:88:f5:9d:ec:b5:c5:db:f0:44:79:
                    d7:4a:43:ab:c4:e5:65:57:79:3b:3d:a2:ac:9e:7c:
                    30:ee:25:18:b9:fe:9c:11:2b:87:ee:a5:20:05:21:
                    7b:20:31:b2:f7:4b:8e:77:d8:d1:39:47:4d:34:7d:
                    5f:c0:55:da:10:3e:be:e9:e7:96:bd:c3:d7:21:cf:
                    ee:cb
                Exponent: 65537 (0x10001)
        X509v3 extensions:
            X509v3 Key Usage: critical
                Digital Signature, Key Encipherment
            X509v3 Extended Key Usage:
                TLS Web Server Authentication, TLS Web Client Authentication
            X509v3 Basic Constraints: critical
                CA:FALSE
            X509v3 Subject Key Identifier:
                94:82:81:4A:8F:87:11:AF:25:63:16:86:AA:39:C5:15:1D:30:E4:06
            X509v3 Authority Key Identifier:
                keyid:9F:80:F4:B3:13:48:27:02:C5:FD:21:DC:AB:E9:23:1E:C6:73:DF:5F

            X509v3 Subject Alternative Name:
                DNS:localhost, IP Address:127.0.0.1, IP Address:0.0.0.0, IP Address:0:0:0:0:0:0:0:1
    Signature Algorithm: sha256WithRSAEncryption
         83:15:bf:21:a0:1b:c8:9d:7e:50:67:c7:84:4e:d7:f4:e7:61:
         1c:25:4a:0a:0c:86:82:60:e4:00:58:b9:f0:8d:a1:51:e6:c1:
         12:47:67:e8:3a:6b:2f:ba:b8:c0:e5:29:94:23:2d:bd:76:01:
         40:b3:62:12:12:8d:42:94:5b:ad:ce:18:c1:4d:d5:fb:60:a4:
         45:7c:64:b6:fb:f7:99:b8:39:07:1c:c2:f9:8a:a5:56:9f:08:
         d9:5c:76:a6:c1:f2:e3:41:d2:f6:41:96:ec:70:91:d3:ae:3d:
         b2:3b:c6:b5:f5:f4:46:dd:b5:9c:36:4c:1e:ae:a7:df:65:43:
         ea:14:50:19:1d:cf:7e:44:24:41:71:58:8a:20:b5:0d:a3:96:
         14:69:75:ba:23:7a:cb:64:7f:ef:5c:53:4b:59:6c:d9:0a:2e:
         b7:2f:65:f3:53:82:ed:94:e2:60:bd:4c:d9:e5:2a:06:39:4b:
         a7:c7:f5:d7:9e:62:f0:86:85:48:66:9a:a5:c2:23:27:7d:cd:
         05:51:20:90:f0:ac:d4:42:cf:4c:36:77:85:0c:98:93:2c:87:
         c6:36:90:ad:d8:4d:ca:00:a7:75:31:56:d8:45:e1:d6:71:0f:
         bf:22:23:9e:47:46:f2:4b:b4:d0:9e:77:52:69:49:88:dc:06:
         8f:2c:d9:3e
```
