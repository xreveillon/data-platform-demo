# Create the certificate for Nessie server

This certificate will be signed by the intermediate CA, and provided to clients to establish the connection, to be trusted by them.

## Create the private key

```bash
openssl genpkey -algorithm RSA -pkeyopt 'rsa_keygen_bits:4096' -out nessie-key.pem -outform PEM
```

## Create the CSR

```bash
openssl req -new -key nessie-key.pem -out nessie.csr -subj '/CN=Nessie server' -addext 'subjectAltName=DNS:nessie,DNS:nessie.edda.lu' -addext 'keyUsage=critical,digitalSignature,keyEncipherment' -addext 'extendedKeyUsage=serverAuth'
```

## Create the certificate signed by the intermediate CA

```bash
openssl x509 -req -days 365 -in nessie.csr -CA ../../02-intermediate-ca/intermediate-ca.pem -CAkey ../../02-intermediate-ca/intermediate-ca-key.pem -CAcreateserial -out nessie.pem -outform PEM -copy_extensions copy
```

## Create the "fullchain" certificate, based on the server and intermediate-ca certificates

```bash
cat nessie.pem ../../02-intermediate-ca/intermediate-ca.pem > nessie-fullchain.pem
```

# Create a PostgreSQL client certificate, signed by the PostgreSQL CA

The same private key will be used.

## Read the .env file

The client certificate must use the postgresql username as CN, so let's read it in the .env file.

```bash
set -a ; source ../../../.env ; set +a
```

## Create the CSR

The name in the CN must be the username in the database.

```bash
openssl req -new -key nessie-key.pem -out nessie-pgclient.csr -subj "/CN=${NESSIE_DB_USERNAME}" -addext 'keyUsage=critical,digitalSignature,keyEncipherment' -addext 'extendedKeyUsage=clientAuth'
```

## Sign the certificate by the PostgreSQL CA

```bash
openssl x509 -req -days 365 -in nessie-pgclient.csr -CA ../../01-root-ca/postgres-ca.pem -CAkey ../../01-root-ca/postgres-ca-key.pem -CAcreateserial -out nessie-pgclient.pem -outform PEM -copy_extensions copy
```

## Put the certificate into a P12 format

```bash
openssl pkcs12 -export -in nessie-pgclient.pem -inkey nessie-key.pem -out nessie-pgclient.p12 -name user -CAfile ../../01-root-ca/postgres-ca.pem -caname postgres-ca -passout "pass:${NESSIE_DB_CERTPASSWORD}"
```

## Quick permission hack for Nessie

```bash
chmod a+r *
```

# Copy here the certificate of the root CA

```bash
cp ../../01-root-ca/root-ca.pem ./
```
