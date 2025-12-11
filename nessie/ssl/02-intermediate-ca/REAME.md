# Create the certificate for the intermediate CA, signed by the root CA

## Create the private key

```bash
openssl genpkey -algorithm RSA -pkeyopt 'rsa_keygen_bits:4096' -out intermediate-ca-key.pem -outform PEM
```

## Create the CSR with all the information

```bash
openssl req -new -key intermediate-ca-key.pem -out intermediate-ca.csr -subj '/CN=Edda Intermediate CA' -addext 'basicConstraints=CA:TRUE,pathlen:0' -addext 'keyUsage=critical,digitalSignature,keyCertSign,cRLSign'
```

## Create the certificate, signed by the root CA

```bash
openssl x509 -req -days 3650 -in intermediate-ca.csr -CA ../01-root-ca/root-ca.pem -CAkey ../01-root-ca/root-ca-key.pem -CAcreateserial -out intermediate-ca.pem -outform PEM -copy_extensions copy
```
