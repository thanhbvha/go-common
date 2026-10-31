#!/usr/bin/env bash
set -euo pipefail

# Disposable integration fixtures only. Never connect this script to an existing
# database. All MongoDB processes, certificates and credentials live in this job.
mode="${1:-replica}"
[[ "$mode" == replica || "$mode" == secure ]] || { echo "Expected replica or secure" >&2; exit 2; }
name="go-common-mongodb-${mode}-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-1}"
image="${MONGODB_TEST_IMAGE:-mongo:8.0.16}"
scratch="$(mktemp -d)"
created_id=""
cleanup() {
  if [[ -n "$created_id" ]]; then docker stop --timeout 10 "$created_id" >/dev/null || true; fi
  # Only the exact, newly created scratch directory is removed.
  if [[ "$scratch" == /tmp/* || "$scratch" == /c/* || "$scratch" == /d/* ]]; then
    rm -f -- "$scratch/ca.pem"
    rmdir -- "$scratch" || true
  fi
}
trap cleanup EXIT
export MSYS_NO_PATHCONV=1
unset MONGODB_TEST_FAILOVER MONGODB_TEST_SECURITY

if [[ "$mode" == replica ]]; then
  created_id="$(docker run --rm -d --name "$name" -p 127.0.0.1:27031:27031 -p 127.0.0.1:27032:27032 -p 127.0.0.1:27033:27033 -p 127.0.0.1:27034:27034 "$image" mongod --port 27031 --replSet go_common_ci --bind_ip_all --setParameter enableTestCommands=1)"
  docker exec "$created_id" sh -c 'mkdir -p /data/n2 /data/n3'
  for port in 27032 27033; do
    dbpath=/data/n2; [[ "$port" == 27033 ]] && dbpath=/data/n3
    docker exec "$created_id" mongod --fork --port "$port" --dbpath "$dbpath" --logpath "/tmp/mongod-${port}.log" --replSet go_common_ci --bind_ip_all --setParameter enableTestCommands=1 >/dev/null
  done
  docker exec "$created_id" sh -c 'mkdir -p /data/standalone; mongod --fork --port 27034 --dbpath /data/standalone --logpath /tmp/standalone.log --bind_ip_all' >/dev/null
  shell=(docker exec "$created_id" mongosh --quiet --port 27031)
else
  created_id="$(docker run --rm -d --name "$name" -p 127.0.0.1:27041:27041 "$image" sleep infinity)"
  # TLS verification remains enabled; this CA signs only the disposable server.
  docker exec "$created_id" sh -c '
    umask 077
    openssl req -x509 -newkey rsa:2048 -nodes -keyout /tmp/ca.key -out /tmp/ca.pem -days 1 -subj /CN=go-common-test-ca 2>/dev/null
    openssl req -newkey rsa:2048 -nodes -keyout /tmp/server.key -out /tmp/server.csr -subj /CN=localhost 2>/dev/null
    printf "subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth,clientAuth\n" > /tmp/server.ext
    openssl x509 -req -in /tmp/server.csr -CA /tmp/ca.pem -CAkey /tmp/ca.key -CAcreateserial -out /tmp/server.crt -days 1 -extfile /tmp/server.ext 2>/dev/null
    cat /tmp/server.crt /tmp/server.key > /tmp/server.pem
    openssl rand -base64 48 > /tmp/replica.key
    chmod 400 /tmp/replica.key
    mkdir -p /data/secure
    mongod --fork --port 27041 --dbpath /data/secure --logpath /tmp/secure.log --bind_ip_all --replSet go_common_ci --keyFile /tmp/replica.key --tlsMode requireTLS --tlsCertificateKeyFile /tmp/server.pem --tlsCAFile /tmp/ca.pem --tlsAllowConnectionsWithoutCertificates --setParameter enableTestCommands=1 >/dev/null
  '
  shell=(docker exec "$created_id" mongosh --quiet --port 27041 --tls --tlsCAFile /tmp/ca.pem)
fi

ready=false
for attempt in {1..30}; do
  if "${shell[@]}" --eval 'quit(db.adminCommand({ping:1}).ok===1?0:1)' >/dev/null 2>&1; then ready=true; break; fi
  sleep 1
done
[[ "$ready" == true ]] || { echo "MongoDB startup timed out" >&2; exit 1; }
if [[ "$mode" == replica ]]; then
  "${shell[@]}" --eval 'rs.initiate({_id:"go_common_ci",members:[{_id:0,host:"localhost:27031"},{_id:1,host:"localhost:27032"},{_id:2,host:"localhost:27033"}]})' >/dev/null
else
  "${shell[@]}" --eval 'rs.initiate({_id:"go_common_ci",members:[{_id:0,host:"localhost:27041"}]})' >/dev/null
fi
"${shell[@]}" --eval 'for(let i=0;i<60;i++){if(db.hello().isWritablePrimary){quit(0)}sleep(500)}quit(1)' >/dev/null

if [[ "$mode" == replica ]]; then
  # Ensure both secondaries are electable before running step-down scenarios.
  "${shell[@]}" --eval 'for(let i=0;i<60;i++){if(rs.status().members.filter(m=>m.stateStr==="SECONDARY").length===2){quit(0)}sleep(500)}quit(1)' >/dev/null
  export MONGODB_INTEGRATION_URI='mongodb://localhost:27031,localhost:27032,localhost:27033/?replicaSet=go_common_ci&serverSelectionTimeoutMS=10000'
  export MONGODB_TEST_FAILOVER=1
  export MONGODB_STANDALONE_URI='mongodb://localhost:27034/?serverSelectionTimeoutMS=5000'
else
  password="$(docker exec "$created_id" openssl rand -hex 24)"
  docker exec -e MONGODB_TEST_PASSWORD="$password" "$created_id" mongosh --quiet --port 27041 --tls --tlsCAFile /tmp/ca.pem --eval 'db.getSiblingDB("admin").createUser({user:"integration",pwd:process.env.MONGODB_TEST_PASSWORD,roles:[{role:"root",db:"admin"}]})' >/dev/null
  ca_path="$scratch/ca.pem"
  if command -v cygpath >/dev/null 2>&1; then ca_path="$(cygpath -m "$ca_path")"; fi
  docker cp "$created_id:/tmp/ca.pem" "$ca_path" >/dev/null
  export MONGODB_INTEGRATION_URI="mongodb://integration:${password}@localhost:27041/?authSource=admin&replicaSet=go_common_ci&tls=true&tlsCAFile=${ca_path}"
  export MONGODB_TEST_SECURITY=1
fi
export MONGODB_TEST_FAILPOINTS=1
# Failover changes the entire fixture: serialize packages so it cannot interrupt
# another package's non-transactional fixture creation.
go test -p 1 -race -tags=integration -count=1 -timeout=180s ./db/mongodb ./examples/db_mongodb
go vet -tags=integration ./db/mongodb ./examples/db_mongodb
