#!/usr/bin/env bash
# Generates the TLS test material in tests/tls: a CA, a server certificate
# for localhost / 127.0.0.1 and a client certificate, both signed by the CA,
# in every store format Saranga reads. Needs keytool (JDK) and openssl.
#
#   tests/tls/make.sh
#
# Store passwords are "changeit"; server.jks has the key password "keypass".
set -euo pipefail
cd "$(dirname "$0")"
rm -f ./*.jks ./*.p12 ./*.pem ./*.pemcfg ./*.crl ./*.cer
export MSYS_NO_PATHCONV=1
P=changeit
DAYS=36500
kt() { keytool -noprompt "$@"; }

# The CA.
kt -genkeypair -alias ca -keyalg RSA -keysize 2048 -dname "CN=Saranga Test CA, O=Saranga" \
   -ext bc:c -validity $DAYS -keystore ca.p12 -storetype PKCS12 -storepass $P
kt -exportcert -rfc -alias ca -keystore ca.p12 -storepass $P -file ca.pem

# A key pair signed by the CA: $1 alias, $2 dname, $3 extensions.
signed() {
  kt -genkeypair -alias "$1" -keyalg RSA -keysize 2048 -dname "$2" -validity $DAYS \
     -keystore "$1.p12" -storetype PKCS12 -storepass $P
  kt -certreq -alias "$1" -keystore "$1.p12" -storepass $P -file "$1.csr"
  kt -gencert -alias ca -keystore ca.p12 -storepass $P -infile "$1.csr" -outfile "$1.cer" \
     -rfc -validity $DAYS $3
  kt -importcert -alias ca -file ca.pem -keystore "$1.p12" -storepass $P
  kt -importcert -alias "$1" -file "$1.cer" -keystore "$1.p12" -storepass $P
  rm -f "$1.csr" "$1.cer"
}
signed server "CN=localhost, OU=Broker, O=Saranga" "-ext SAN=dns:localhost,ip:127.0.0.1 -ext EKU=serverAuth"
signed client "CN=client, OU=Test, O=Saranga" "-ext EKU=clientAuth"
signed other "CN=other, OU=Test, O=Saranga" "-ext EKU=clientAuth"

# server.p12 in the other formats.
kt -importkeystore -srckeystore server.p12 -srcstorepass $P -destkeystore server.jks \
   -deststoretype JKS -deststorepass $P -destkeypass keypass -srcalias server -destalias server
kt -J-Dkeystore.pkcs12.legacy -importkeystore -srckeystore server.p12 -srcstorepass $P \
   -destkeystore server-legacy.p12 -deststoretype PKCS12 -deststorepass $P
openssl pkcs12 -in server.p12 -passin pass:$P -nocerts -nodes 2>/dev/null \
  | openssl pkey -out server-key.pem
openssl pkcs12 -in server.p12 -passin pass:$P -clcerts -nokeys 2>/dev/null \
  | openssl x509 -out server-cert.pem
cat server-key.pem server-cert.pem ca.pem > server.pem
printf 'alias=server\nsource.key=server-key.pem\nsource.cert=server-cert.pem\n' > server.pemcfg
openssl pkcs12 -in client.p12 -passin pass:$P -nocerts -nodes 2>/dev/null \
  | openssl pkey -out client-key.pem
openssl pkcs12 -in client.p12 -passin pass:$P -clcerts -nokeys 2>/dev/null \
  | openssl x509 -out client-cert.pem

# Truststores holding the CA.
kt -importcert -alias ca -file ca.pem -keystore truststore.jks -storetype JKS -storepass $P
kt -importcert -alias ca -file ca.pem -keystore truststore.p12 -storetype PKCS12 -storepass $P
rm -f server-legacy.p12.old

# A truststore that trusts something else entirely.
kt -genkeypair -alias stranger -keyalg RSA -keysize 2048 -dname "CN=Stranger CA" -ext bc:c    -validity $DAYS -keystore stranger.p12 -storetype PKCS12 -storepass $P
kt -exportcert -alias stranger -keystore stranger.p12 -storepass $P -file stranger.cer
kt -importcert -alias stranger -file stranger.cer -keystore untrusted.p12 -storetype PKCS12 -storepass $P
rm -f stranger.p12 stranger.cer

# A CRL revoking "other": the CA's key, as PEM, with openssl's CA tool.
D=crl.work && rm -rf "$D" && mkdir "$D"
openssl pkcs12 -in ca.p12 -passin pass:$P -nocerts -nodes 2>/dev/null | openssl pkey -out "$D/ca-key.pem"
openssl pkcs12 -in other.p12 -passin pass:$P -clcerts -nokeys 2>/dev/null | openssl x509 -out "$D/other.pem"
mkdir -p "$D/db" && touch "$D/db/index.txt" && echo 1000 > "$D/db/crlnumber"
cat > "$D/ca.cnf" <<EOF
[ca]
default_ca = test
[test]
database = $D/db/index.txt
crlnumber = $D/db/crlnumber
default_md = sha256
default_crl_days = $DAYS
EOF
openssl ca -config "$D/ca.cnf" -keyfile "$D/ca-key.pem" -cert ca.pem -revoke "$D/other.pem" 2>/dev/null
openssl ca -config "$D/ca.cnf" -keyfile "$D/ca-key.pem" -cert ca.pem -gencrl -out revoked.crl 2>/dev/null
rm -rf "$D"
ls -l
