# Assembling the root CA certificate

## Create the private key

```bash
openssl genpkey -algorithm RSA -pkeyopt 'rsa_keygen_bits:4096' -out root-ca-key.pem -outform PEM
```

## Create the CSR with all the information

```bash
openssl req -new -key root-ca-key.pem -out root-ca.csr -subj '/CN=Edda ROOT CA' -addext 'basicConstraints=CA:TRUE,pathlen:2' -addext 'keyUsage=critical,digitalSignature,keyCertSign,cRLSign'
```

## Create the root CA certificate by signing the CSR with the private key

```bash
openssl x509 -req -days 10950 -in root-ca.csr -signkey root-ca-key.pem -out root-ca.pem -outform PEM -copy_extensions copyAll
```


# Create a CA for PostgreSQL

This certificate will sign client certificates and authenticate them.

## Create the private key

```bash
openssl genpkey -algorithm RSA -pkeyopt 'rsa_keygen_bits:4096' -out postgres-ca-key.pem -outform PEM
```

## Create the CSR with all the information

```bash
openssl req -new -key postgres-ca-key.pem -out postgres-ca.csr -subj '/CN=Edda PostgreSQL CA' -addext 'basicConstraints=CA:TRUE,pathlen:0' -addext 'keyUsage=critical,digitalSignature,keyCertSign,cRLSign'
```

## Create the PostgreSQL CA certificate by signing the CSR with the private key

```bash
openssl x509 -req -days 10950 -in postgres-ca.csr -signkey postgres-ca-key.pem -out postgres-ca.pem -outform PEM -copy_extensions copyAll
```
