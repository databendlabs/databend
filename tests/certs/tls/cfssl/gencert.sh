

function generate_keys() {
    # generate rsa key pairs for CA, databend server and databend client
    (cd ca; cfssl gencert -initca ca-csr.json | cfssljson -bare ca -)
    (cd server; cfssl gencert -ca=../ca/ca.pem -ca-key=../ca/ca-key.pem -config=../ca/ca-config.json -profile=server server.json | cfssljson -bare server)
    (cd client; cfssl gencert -ca=../ca/ca.pem -ca-key=../ca/ca-key.pem -config=../ca/ca-config.json -profile=client client.json | cfssljson -bare client)

    # encrypt key pairs to pkcs8 form
    (cd ca; openssl pkcs8 -topk8 -nocrypt -in ca-key.pem -out pkcs8-ca-key.pem)
    (cd client; openssl pkcs8 -topk8 -nocrypt -in client-key.pem -out pkcs8-client-key.pem)
    (cd server; openssl pkcs8 -topk8 -nocrypt -in server-key.pem -out pkcs8-server-key.pem)

    (cd client;  openssl pkcs12 -export -out client-identity.pfx -inkey pkcs8-client-key.pem -in client.pem -certfile ../ca/ca.pem -password pass:databend)
}

function clean_tmp_keys() {
   (cd ca; rm -rf *.pem; rm -rf *.csr)
   (cd server; rm -rf *.pem; rm -rf *.csr)
   (cd client; rm -rf *.pem; rm -rf *.csr; rm -r *.pfx)
}

need_cmd() {
    if ! check_cmd "$1"; then
        echo "need '$1' (command not found)"
        exit 1
    fi
}

check_cmd() {
    command -v "$1" > /dev/null 2>&1
}

need_cmd cfssl
need_cmd openssl

# TODO add usage and help message for new coming contributors
"$@"