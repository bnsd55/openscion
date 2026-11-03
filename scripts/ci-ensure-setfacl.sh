#!/bin/sh
# The checked open's real-ACL case must run in CI, not skip:
# SCION_TEST_REQUIRE_SETFACL=1 (set by the jobs that call this) turns a
# missing setfacl into a test failure, so this step installs the acl
# package when the image lacks it, and the log says which way it went.
set -eu
if command -v setfacl > /dev/null; then
    echo "setfacl is present: $(command -v setfacl)"
else
    echo "setfacl is missing: installing the acl package"
    sudo apt-get update -q
    sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -q acl
    command -v setfacl
fi
