#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CERT_DIR="${ROOT_DIR}/certs"

mkdir -p "${CERT_DIR}"

cat > "${CERT_DIR}/openssl.cnf" <<'CONF'
[ req ]
default_bits       = 2048
distinguished_name = req_distinguished_name
req_extensions     = req_ext
prompt             = no

[ req_distinguished_name ]
CN = worker-admin

[ req_ext ]
subjectAltName = @alt_names

[ alt_names ]
DNS.1 = worker-admin
DNS.2 = localhost
IP.1 = 127.0.0.1
CONF

openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
  -keyout "${CERT_DIR}/ca.key" -out "${CERT_DIR}/ca.crt" \
  -subj "/CN=worker-admin-ca"

echo "01" > "${CERT_DIR}/ca.srl"

openssl req -new -nodes -newkey rsa:2048 \
  -keyout "${CERT_DIR}/server.key" -out "${CERT_DIR}/server.csr" \
  -config "${CERT_DIR}/openssl.cnf"

openssl x509 -req -in "${CERT_DIR}/server.csr" \
  -CA "${CERT_DIR}/ca.crt" -CAkey "${CERT_DIR}/ca.key" -CAserial "${CERT_DIR}/ca.srl" \
  -out "${CERT_DIR}/server.crt" -days 365 \
  -extensions req_ext -extfile "${CERT_DIR}/openssl.cnf"

openssl req -new -nodes -newkey rsa:2048 \
  -keyout "${CERT_DIR}/client.key" -out "${CERT_DIR}/client.csr" \
  -subj "/CN=worker-admin-client"

openssl x509 -req -in "${CERT_DIR}/client.csr" \
  -CA "${CERT_DIR}/ca.crt" -CAkey "${CERT_DIR}/ca.key" -CAserial "${CERT_DIR}/ca.srl" \
  -out "${CERT_DIR}/client.crt" -days 365

rm -f "${CERT_DIR}/server.csr" "${CERT_DIR}/client.csr" "${CERT_DIR}/openssl.cnf" "${CERT_DIR}/ca.srl"

echo "Generated certs in ${CERT_DIR}"
