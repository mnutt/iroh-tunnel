#!/bin/bash

# When you change this file, you must take manual action. Read this doc:
# - https://docs.sandstorm.io/en/latest/vagrant-spk/customizing/#setupsh

set -euo pipefail
# This is the ideal place to do things like:
#
#    export DEBIAN_FRONTEND=noninteractive
#    apt-get update
#    apt-get install -y nginx nodejs nodejs-legacy python2.7 mysql-server
#
# If the packages you're installing here need some configuration adjustments,
# this is also a good place to do that:
#
#    sed --in-place='' \
#            --expression 's/^user www-data/#user www-data/' \
#            --expression 's#^pid /run/nginx.pid#pid /var/run/nginx.pid#' \
#            --expression 's/^\s*error_log.*/error_log stderr;/' \
#            --expression 's/^\s*access_log.*/access_log off;/' \
#            /etc/nginx/nginx.conf

export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y build-essential capnproto

export RUSTUP_HOME=/opt/rustup/rustup
export CARGO_HOME=/opt/rustup/cargo

APP_USER="${SUDO_USER:-}"
if [ -z "${APP_USER}" ] || [ "${APP_USER}" = "root" ]; then
  if id -u vagrant >/dev/null 2>&1; then
    APP_USER="vagrant"
  else
    APP_USER="$(find /home -mindepth 1 -maxdepth 1 -type d -exec basename {} \; | head -n1)"
  fi
fi

if [ ! -x /opt/rustup/cargo/bin/rustc ]; then
  mkdir -p /opt/rustup
  export RUSTUP_INIT_SKIP_PATH_CHECK=yes
  curl https://sh.rustup.rs -sSf | sh -s -- -y --profile minimal --default-toolchain stable --no-modify-path
fi

if [ -n "${APP_USER}" ] && id -u "${APP_USER}" >/dev/null 2>&1; then
  chown -R "${APP_USER}:${APP_USER}" /opt/rustup
fi

exit 0
