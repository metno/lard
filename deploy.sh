#!/bin/bash

if ! cargo build --workspace --release; then
  exit 1
fi

pushd ansible || exit

cp ../target/release/lard_ingestion roles/deploy/files/.
cp ../ingestion/resources roles/deploy/files/.

ansible-playbook -i inventory.yml deploy.yml

popd || exit
