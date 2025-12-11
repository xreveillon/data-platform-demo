# Create the certificate for PostgreSQL server

This certificate will be signed by the intermediate CA, and provided to clients to establish the connection, to be trusted by them.

## Create the private key

```bash
openssl genpkey -algorithm RSA -pkeyopt 'rsa_keygen_bits:4096' -out nessie-db-key.pem -outform PEM
```

## Create the CSR

```bash
openssl req -new -key nessie-db-key.pem -out nessie-db.csr -subj '/CN=PostgreSQL server' -addext 'subjectAltName=DNS:nessie-db,DNS:nessie-db.edda.lu' -addext 'keyUsage=critical,digitalSignature,keyEncipherment' -addext 'extendedKeyUsage=serverAuth'
```

## Create the certificate signed by the intermediate CA

```bash
openssl x509 -req -days 365 -in nessie-db.csr -CA ../../02-intermediate-ca/intermediate-ca.pem -CAkey ../../02-intermediate-ca/intermediate-ca-key.pem -CAcreateserial -out nessie-db.pem -outform PEM -copy_extensions copy
```

## Create the "fullchain" certificate, based on the server and intermediate-ca certificates

```bash
cat nessie-db.pem ../../02-intermediate-ca/intermediate-ca.pem > nessie-db-fullchain.pem
```

# Get the CA certificate

```bash
cp ../../01-root-ca/postgres-ca.pem ./
```
