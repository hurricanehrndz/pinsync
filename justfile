# pinsync task runner. Later phases add build/test recipes here.

# Stand up the Roles Anywhere test infrastructure (idempotent).
ra-infra-up:
    ./hack/ra-test/up.sh

# Tear down the Roles Anywhere test infrastructure.
ra-infra-down:
    ./hack/ra-test/down.sh
