#!/bin/sh
# The shellcheck gate for scripts/ (#1369): a script mistake must fail a
# check on the PR that carries it, the way a workflow that cannot parse
# does (lint-workflows.sh). actionlint only covers the workflows' own run
# blocks; nothing read scripts/*.sh before this.
#
# The script needs: curl (or a PATH shellcheck of the pinned version),
# shasum or sha256sum, and xz. It fetches a pinned shellcheck release
# (its sha256 is pinned here and checked before it runs) unless the PATH
# already has that exact version - the runner image's shellcheck can
# change under us with no commit in this repo, so a PATH binary of any
# other version does not count.
set -eu

# The pins. Bump a version together with its sha256 lines, computed from
# the release assets at bump time.
SHELLCHECK_VERSION=0.11.0
SHELLCHECK_LINUX_X86_64=8c3be12b05d5c177a04c29e3c78ce89ac86f1595681cab149b65b97c4e227198
SHELLCHECK_DARWIN_AARCH64=56affdd8de5527894dca6dc3d7e0a99a873b0f004d7aabc30ae407d3f48b0a79

note() { printf '%s %s\n' "lint-scripts:" "$*"; }
die() { printf '%s %s\n' "lint-scripts:" "$*" >&2; exit 1; }

# The release URL's base. A test or a local run can point it elsewhere;
# unset, it is the real release host.
url_base=${SHELLCHECK_URL_BASE:-https://github.com/koalaman/shellcheck/releases/download}

# A PATH shellcheck counts only at the pinned version.
if command -v shellcheck >/dev/null 2>&1 \
  && [ "$(shellcheck --version | sed -n 's/^version: //p')" = "$SHELLCHECK_VERSION" ]; then
  SHELLCHECK=$(command -v shellcheck)
  note "using the PATH shellcheck $SHELLCHECK_VERSION"
else
  case "$(uname -s)-$(uname -m)" in
    Linux-x86_64) asset=linux.x86_64; want=$SHELLCHECK_LINUX_X86_64 ;;
    Darwin-arm64) asset=darwin.aarch64; want=$SHELLCHECK_DARWIN_AARCH64 ;;
    *) die "no pinned shellcheck for $(uname -s)-$(uname -m)" ;;
  esac
  dir=${SHELLCHECK_DIR:-${TMPDIR:-/tmp}/shellcheck-$SHELLCHECK_VERSION-bin}
  mkdir -p "$dir"
  tarball=$dir/shellcheck.tar.xz
  if [ ! -x "$dir/shellcheck" ]; then
    url="$url_base/v$SHELLCHECK_VERSION/shellcheck-v$SHELLCHECK_VERSION.$asset.tar.xz"
    note "fetching the pinned shellcheck $SHELLCHECK_VERSION"
    curl -fsSL -o "$tarball" "$url"
    if command -v sha256sum >/dev/null 2>&1; then
      got=$(sha256sum "$tarball" | cut -d' ' -f1)
    else
      got=$(shasum -a 256 "$tarball" | cut -d' ' -f1)
    fi
    [ "$got" = "$want" ] || die "the shellcheck tarball's sha256 is $got, not the pinned $want"
    tar -xJf "$tarball" -C "$dir" shellcheck-v$SHELLCHECK_VERSION/shellcheck
    mv "$dir/shellcheck-v$SHELLCHECK_VERSION/shellcheck" "$dir/shellcheck"
    rmdir "$dir/shellcheck-v$SHELLCHECK_VERSION"
  fi
  SHELLCHECK=$dir/shellcheck
  note "using the pinned shellcheck $SHELLCHECK_VERSION"
fi

# Every scripts/*.sh but capacity.sh, which two open pull requests touch
# (#1363, #1365): it carries SC2318 and SC2086 findings those PRs'
# changes resolve, and it joins this check when they merge.
cd "$(dirname "$0")/.."
find scripts -maxdepth 1 -name '*.sh' ! -name capacity.sh -print0 |
  xargs -0 "$SHELLCHECK" -x

# The bash 3.2 gate (#1395): macOS runs the scripts under /bin/bash
# 3.2, so a bash 4 form must fail this job in seconds, not a macOS test
# job 20 minutes in (#1388). check-bash3.sh names the forms and their
# reason; it sits out of its own scan, because it names them, and so
# does its test, whose fixtures are the forms themselves.
find scripts -maxdepth 1 -name '*.sh' ! -name capacity.sh \
  ! -name check-bash3.sh ! -name test-check-bash3.sh -print0 |
  xargs -0 scripts/check-bash3.sh
