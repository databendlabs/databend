# Test Certs

We use certs inside `cfssl` for tls testing.

Please use `./gencert.sh generate_keys` to refresh all the certs.

## Current Used

The next refresh time will be `Sep 14 09:09:00 2024 GMT`

```shell
:) openssl x509 -noout -text -in ./tests/certs/tls/cfssl/server/server.pem
Certificate:
    Data:
        Version: 3 (0x2)
        Serial Number:
            65:08:78:3f:75:71:8a:08:56:5f:26:d2:c8:db:61:66:a2:00:7a:fd
    Signature Algorithm: sha256WithRSAEncryption
        Issuer: C=US, ST=LA, L=CA, O=Datafuselabs, OU=Databend, CN=Databend
        Validity
            Not Before: Sep 15 09:09:00 2023 GMT
            Not After : Sep 14 09:09:00 2024 GMT
        Subject: C=US, ST=CA, L=San Francisco, CN=Databend Server
        Subject Public Key Info:
            Public Key Algorithm: rsaEncryption
                RSA Public-Key: (2048 bit)
                Modulus:
                    00:bc:36:22:ab:a1:8c:c2:04:c6:e6:d2:22:ea:a8:
                    4e:21:62:44:16:e2:bd:e2:dd:31:da:c7:87:de:9a:
                    6d:03:f8:08:02:2b:4b:7c:32:3a:81:e7:2b:68:97:
                    9c:f0:7d:50:09:f6:f4:72:ab:ef:ff:86:16:da:da:
                    d0:2a:5e:6b:6f:fe:b6:42:6d:ed:bb:dc:2a:ed:50:
                    51:bc:74:06:db:ba:fd:ac:2f:f6:73:4d:14:17:0c:
                    94:91:e2:b0:fe:e4:4a:f4:af:5e:d7:9e:b2:0b:e9:
                    28:50:87:32:71:1d:ef:a2:82:e0:e3:30:54:94:f8:
                    3e:65:f3:c7:5a:c9:91:86:8b:30:71:b6:12:b3:48:
                    bf:c9:c3:25:2e:5e:6f:16:15:34:cb:93:14:98:2e:
                    fd:e7:4a:e5:f8:57:62:c6:80:73:cf:b4:a2:02:87:
                    2f:ec:04:36:1e:2e:31:b7:f3:be:10:21:df:6f:72:
                    c9:bb:f0:fd:c7:70:78:4f:a2:3f:db:e2:f0:31:2a:
                    9e:2b:76:3a:4d:57:0a:ba:00:1e:cc:75:79:4c:1a:
                    1c:67:26:df:e4:93:c3:08:0e:54:a7:5c:fa:1a:85:
                    45:31:34:6b:77:be:fe:6f:6f:f8:45:90:f3:ee:bb:
                    a5:4b:15:08:c3:8b:50:3b:4b:32:e8:5e:e9:e5:37:
                    4f:e1
                Exponent: 65537 (0x10001)
        X509v3 extensions:
            X509v3 Key Usage: critical
                Digital Signature, Key Encipherment
            X509v3 Extended Key Usage: 
                TLS Web Server Authentication, TLS Web Client Authentication
            X509v3 Basic Constraints: critical
                CA:FALSE
            X509v3 Subject Key Identifier: 
                26:B7:1C:6F:DD:34:86:82:37:4C:BC:8C:B3:DE:AE:E2:A1:80:D5:56
            X509v3 Authority Key Identifier: 
                keyid:12:1C:A8:35:73:6D:44:3C:60:44:15:EB:7D:69:28:72:D8:22:D7:F8

            X509v3 Subject Alternative Name: 
                DNS:localhost, IP Address:127.0.0.1, IP Address:0.0.0.0, IP Address:0:0:0:0:0:0:0:1
    Signature Algorithm: sha256WithRSAEncryption
         32:b4:f1:48:df:97:14:ce:b7:01:0c:04:a1:22:11:bd:34:1d:
         ff:5d:a0:76:69:4f:5c:2e:c2:08:8a:00:03:ac:c8:a9:63:7f:
         d6:11:af:6a:b8:51:94:f9:1d:17:02:b5:4d:e0:09:f3:d2:7e:
         bd:c3:0d:01:b2:94:e2:cb:24:da:ef:90:21:6b:46:e2:78:3e:
         11:80:08:63:2f:6d:40:4f:d1:23:0e:e2:20:b5:f8:b5:49:02:
         27:8f:bf:eb:02:91:52:ad:44:f6:82:57:6a:8c:8d:8a:ee:ab:
         34:ab:d5:6a:d7:28:e9:c8:1c:ca:6c:33:94:0e:a2:07:a0:96:
         2e:8e:a1:1b:18:3b:32:78:36:25:03:3f:be:ff:a7:63:68:af:
         8a:64:e4:8a:c5:5f:bc:20:f9:7b:d0:8c:91:84:a4:52:53:93:
         a5:0b:1c:09:04:4e:fd:1b:e9:18:10:8b:ce:29:47:f8:d9:2d:
         ba:e7:09:a5:de:ef:a4:45:43:9f:45:7c:19:6e:ff:64:61:6e:
         e2:6c:83:4a:5f:a2:87:d3:b3:bd:88:04:b1:12:68:7d:42:ee:
         df:c6:6c:a1:ec:0f:88:90:2a:ce:81:85:8e:32:c7:e3:d4:03:
         05:17:e0:78:a5:45:43:26:56:df:29:d6:f0:ca:1e:97:6b:b7:
         cf:42:23:df
```
