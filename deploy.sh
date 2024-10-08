#!/bin/bash

if ! cargo build --workspace --release; then
    exit 1
fi

pushd ansible || exit

ansible-playbook -i inventory.yml deploy.yml --ask-vault-pass

popd || exit
