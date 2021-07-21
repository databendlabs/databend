#!/bin/bash
set -e
# Copyright 2020-2021 The Datafuse Authors.
# SPDX-License-Identifier: Apache-2.0.
usage() {
  this=$1
  cat <<EOF
$this: download rust binaries for datafuselabs/datafuse
Usage: $this [-b] bindir [-d] [tag]
  -b sets bindir or installation directory, Defaults to {HOME}/.datafuse/bin
  -d turns on debug logging
   [tag] is a tag from
   https://github.com/datafuselabs/datafuse/releases
   If tag is missing, then the latest will be used.
EOF
  exit 2
}

parse_args() {
  #BINDIR is ${HOME}/.datafuse/bin unless set be ENV
  # over-ridden by flag below

  BINDIR=${BINDIR:-.datafuse/bin}
  while getopts "b:dh?x" arg; do
    case "$arg" in
      b) BINDIR="$OPTARG" ;;
      d) log_set_priority 10 ;;
      h | \?) usage "$0" ;;
      x) set -x ;;
    esac
  done
  shift $((OPTIND - 1))
  TAG=$1
}
detect_rosetta() {
  local _cpu=$1; shift
  if [ "${_cpu}" = "x86_64" ]; then
    if ! [ -x "$(command -v sysctl)" ]; then
      return 1
    fi
    if [ "$(sysctl -in sysctl.proc_translated 2>/dev/null)" = "1" ]; then
        log_info "Running on Rosetta 2"
        return 0
    else
        return 1
    fi
  fi
  return 1
}

check_proc() {
    # Check for /proc by looking for the /proc/self/exe link
    # This is only run on Linux
    if ! test -L /proc/self/exe ; then
        err "fatal: Unable to find /proc/self/exe.  Is /proc mounted?  Installation cannot proceed without /proc."
    fi
}

# problematic in arm/v7 and arm/v6 environment
get_bitness() {
    need_cmd head
    # Architecture detection without dependencies beyond coreutils.
    # ELF files start out "\x7fELF", and the following byte is
    #   0x01 for 32-bit and
    #   0x02 for 64-bit.
    # The printf builtin on some shells like dash only supports octal
    # escape sequences, so we use those.
    local _current_exe_head
    _current_exe_head=$(head -c 5 /proc/self/exe )
    if [ "$_current_exe_head" = "$(printf '\177ELF\001')" ]; then
        echo 32
    elif [ "$_current_exe_head" = "$(printf '\177ELF\002')" ]; then
        echo 64
    else
        log_err "unknown platform bitness"
    fi
}

# problematic in arm/v7 and arm/v6 environment
get_endianness() {
    local cputype=$1
    local suffix_eb=$2
    local suffix_el=$3

    # detect endianness without od/hexdump, like get_bitness() does.
    need_cmd head
    need_cmd tail

    local _current_exe_endianness
    _current_exe_endianness="$(head -c 6 /proc/self/exe | tail -c 1)"
    if [ "$_current_exe_endianness" = "$(printf '\001')" ]; then
        echo "${cputype}${suffix_el}"
    elif [ "$_current_exe_endianness" = "$(printf '\002')" ]; then
        echo "${cputype}${suffix_eb}"
    else
        log_err "unknown platform endianness"
    fi
}
# Cross-platform architecture detection, borrowed from rustup-init.sh
get_architecture() {
    local _ostype _cputype _bitness _arch _clibtype _is_rosseta
    _ostype="$(uname -s)"
    _cputype="$(uname -m)"
    _clibtype="gnu"
    # rosetta hack, return ARM64 binary directly
    detect_rosetta "$_cputype"
    _is_rosseta=$?
    if [ $_is_rosseta -eq 0 ]; then
        log_err "⚠️ Macbook M1 is not officially supported!"
        _arch="aarch64-apple-darwin"
        RETVAL="$_arch"
        return
    fi
    if [ "$_ostype" = Linux ]; then
        if [ "$(uname -o)" = Android ]; then
            _ostype=Android
        fi
        if ldd --version 2>&1 | grep -q 'musl'; then
            _clibtype="musl"
        fi
    fi

    if [ "$_ostype" = Darwin ] && [ "$_cputype" = i386 ]; then
        # Darwin `uname -m` lies
        if sysctl hw.optional.x86_64 | grep -q ': 1'; then
            _cputype=x86_64
        fi
    fi

    if [ "$_ostype" = SunOS ]; then
        # Both Solaris and illumos presently announce as "SunOS" in "uname -s"
        # so use "uname -o" to disambiguate.  We use the full path to the
        # system uname in case the user has coreutils uname first in PATH,
        # which has historically sometimes printed the wrong value here.
        if [ "$(/usr/bin/uname -o)" = illumos ]; then
            _ostype=illumos
        fi

        # illumos systems have multi-arch userlands, and "uname -m" reports the
        # machine hardware name; e.g., "i86pc" on both 32- and 64-bit x86
        # systems.  Check for the native (widest) instruction set on the
        # running kernel:
        if [ "$_cputype" = i86pc ]; then
            _cputype="$(isainfo -n)"
        fi
    fi

    case "$_ostype" in

        Android)
            _ostype=linux-android
            ;;

        Linux)
            check_proc
            _ostype=unknown-linux-$_clibtype
            _bitness=$(get_bitness)
            ;;

        FreeBSD)
            _ostype=unknown-freebsd
            ;;

        NetBSD)
            _ostype=unknown-netbsd
            ;;

        DragonFly)
            _ostype=unknown-dragonfly
            ;;

        Darwin)
            _ostype=apple-darwin
            ;;

        illumos)
            _ostype=unknown-illumos
            ;;

        MINGW* | MSYS* | CYGWIN*)
            _ostype=pc-windows-gnu
            ;;

        *)
            log_err "unrecognized OS type: $_ostype"
            ;;

    esac

    case "$_cputype" in

        i386 | i486 | i686 | i786 | x86)
            _cputype=i686
            ;;

        xscale | arm)
            _cputype=arm
            if [ "$_ostype" = "linux-android" ]; then
                _ostype=linux-androideabi
            fi
            ;;

        armv6l)
            _cputype=arm
            if [ "$_ostype" = "linux-android" ]; then
                _ostype=linux-androideabi
            else
                _ostype="${_ostype}eabihf"
            fi
            ;;

        armv7l | armv8l)
            _cputype=armv7
            if [ "$_ostype" = "linux-android" ]; then
                _ostype=linux-androideabi
            else
                _ostype="${_ostype}eabihf"
            fi
            ;;

        aarch64 | arm64)
            _cputype=aarch64
            ;;

        x86_64 | x86-64 | x64 | amd64)
            _cputype=x86_64
            ;;

        mips)
            _cputype=$(get_endianness mips '' el)
            ;;

        mips64)
            if [ "$_bitness" -eq 64 ]; then
                # only n64 ABI is supported for now
                _ostype="${_ostype}abi64"
                _cputype=$(get_endianness mips64 '' el)
            fi
            ;;

        ppc)
            _cputype=powerpc
            ;;

        ppc64)
            _cputype=powerpc64
            ;;

        ppc64le)
            _cputype=powerpc64le
            ;;

        s390x)
            _cputype=s390x
            ;;
        riscv64)
            _cputype=riscv64gc
            ;;
        *)
            log_err "unknown CPU type: $_cputype"

    esac

    # Detect 64-bit linux with 32-bit userland
    if [ "${_ostype}" = unknown-linux-gnu ] && [ "${_bitness}" -eq 32 ]; then
        case $_cputype in
            x86_64)
                _cputype=i686
                ;;
            mips64)
                _cputype=$(get_endianness mips '' el)
                ;;
            powerpc64)
                _cputype=powerpc
                ;;
            aarch64)
                _cputype=armv7
                if [ "$_ostype" = "linux-android" ]; then
                    _ostype=linux-androideabi
                else
                    _ostype="${_ostype}eabihf"
                fi
                ;;
            riscv64gc)
                log_err "riscv64 with 32-bit userland unsupported"
                ;;
        esac
    fi

    # Detect armv7 but without the CPU features Rust needs in that build,
    # and fall back to arm.
    # See https://github.com/rust-lang/rustup.rs/issues/587.
    if [ "$_ostype" = "unknown-linux-gnueabihf" ] && [ "$_cputype" = armv7 ]; then
        if ensure grep '^Features' /proc/cpuinfo | grep -q -v neon; then
            # At least one processor does not have NEON.
            _cputype=arm
        fi
    fi

    _arch="${_cputype}-${_ostype}"

    RETVAL="$_arch"
}
assert_nz() {
    if [ -z "$1" ]; then
        log_err "assert_nz $2"
        exit 1
    fi
}
log_prefix() {
  echo "$0"
}
echoerr() {
  echo "$@"
}

log_debug() {
  log_priority 7 || return 0
  echoerr "$(log_prefix)" "$(log_tag 7)" "$@"
}
log_err() {
  log_priority 3 || return 0
  echoerr "$(log_prefix)" "$(log_tag 3)" "$@"
}
_logp=6
log_set_priority() {
  _logp="$1"
}
log_priority() {
  if test -z "$1"; then
    echo "$_logp"
    return
  fi
  [ "$1" -le "$_logp" ]
}
log_tag() {
  case $1 in
    0) echo "emerg" ;;
    1) echo "alert" ;;
    2) echo "crit" ;;
    3) echo "err" ;;
    4) echo "warning" ;;
    5) echo "notice" ;;
    6) echo "info" ;;
    7) echo "debug" ;;
    *) echo "$1" ;;
  esac
}

log_info() {
  log_priority 6 || return 0
  echoerr "$(log_prefix)" "$(log_tag 6)" "$@"
}

log_crit() {
  log_priority 2 || return 0
  echoerr "$(log_prefix)" "$(log_tag 2)" "$@"
}

# Ensure that this architecture is supported and matches the
# naming convention of known platform releases in the registry
#
# @param $1: The target triple of this architecture
# @return: Status 0 if the architecture is supported, exit if not
assert_supported_architecture() {
    local _arch="$1"; shift

    # Match against all supported architectures
    case $_arch in
        x86_64-apple-darwin)
            echo "x86_64-apple-darwin"
            return 0
            ;;
        aarch64-unknown-linux-gnu)
            echo "aarch64-unknown-linux-gnu"
            return 0
            ;;
        x86_64-unknown-linux-gnu)
            echo "x86_64-unknown-linux-gnu"
            return 0
            ;;
        arm-unknown-linux-gnueabi)
            echo "arm-unknown-linux-gnueabi"
            return 0
            ;;
        arm-unknown-linux-gnueabihf)
            echo "arm-unknown-linux-gnueabi" # armv7 hf not work
            return 0
            ;;
        armv7-unknown-linux-gnueabihf)
            echo "armv7-unknown-linux-gnueabihf"
            return 0
            ;;
        *)
          log_err "current architecture $_arch is not supported, Make sure this script is up-to-date and file request at https://github.com/$DATAFUSE_REPO/issues/new"
          return 1
          ;;
    esac

    return 1
}
get_latest_tag() {
  # shellcheck disable=SC2046
  curl --silent "https://api.github.com/repos/$1/tags"  |  grep -Eo '"name"[^,]*' | sed -r 's/^[^:]*:(.*)$/\1/' | head -n 1 | sed -e 's/^[[:space:]]*//' | sed -e 's/[[:space:]]*$//'
}

# Untar release binary files
# @param $1: The target tar file
# @return: Status 0 if the architecture is supported, exit if not
untar() {
  local tarball=$1; shift
  case "${tarball}" in
    *.tar.gz | *.tgz) tar --no-same-owner -xzf "${tarball}" ;;
    *.tar) tar --no-same-owner -xf "${tarball}" ;;
    *.zip) unzip "${tarball}" ;;
    *)
      log_err "untar unknown archive format for ${tarball}"
      return 1
      ;;
  esac
}

# Exit immediately, prompting the user to file an issue on GH
abort_prompt_issue() {
    log_err ""
    log_err "If you believe this is a bug (or just need help),"
    log_err "please feel free to file an issue on Github ❤️"
    log_err "    https://github.com/$DATAFUSE_REPO/issues/new"
    exit 1
}

set_tag() {
  local _tag="$TAG"; shift
  if [ -z "$_tag" ]; then
      _tag=$(get_latest_tag "$DATAFUSE_REPO") || return 1
  fi

  TAG=$(echo "$_tag" | tr -d '"')
}

# Download datafuse compressed file to a temp file
#
# @param $1: The URL of the file to download to a temporary dir
# @return <stdout>: The path of the temporary file downloaded
download_datafuse() {
    local _status
    local _name="$1";
    local _url="$2"; shift
    tmpdir=$(mktemp -d)
    log_debug "downloading files into ${tmpdir}"
    log_info "😊 Start to download datafuse in ${_url}"
    http_download "${tmpdir}/${_name}" "${_url}"
    _status=$?
    if [ $_status -ne 0 ]; then
        log_err "❌ Failed to download datafuse!"
        log_err "    Error downloading from ${_url}"
        rm -rf tmpdir
        abort_prompt_issue
    fi
  log_info "✅ Successfully downloaded datafuse in ${_url}"
    srcdir="${tmpdir}"
    (cd "${tmpdir}" && untar "${_name}")
    _status=$?
    if [ $_status -ne 0 ]; then
        log_err "❌ Failed to unzip datafuse!"
        log_err "    Error from untar ${_name}"
        rm -rf tmpdir
        abort_prompt_issue
    fi
    echo "${HOME}/${BINDIR}"
    test ! -d "${HOME}/${BINDIR}" && install -d "${HOME}/${BINDIR}"
    for binexe in fuse-query fuse-store; do
      #TODO(zhihanz) for windows we should add .exe suffix
      install "${srcdir}/${binexe}" "${HOME}/${BINDIR}/"
      ensure chmod +x "${HOME}/${BINDIR}/${binexe}"
      log_info "✅ Successfully installed ${HOME}/${BINDIR}/${binexe}"
    done
    rm -rf "${tmpdir}"
    return $_status
}

http_download_curl() {
  local_file=$1
  source_url=$2
  header=$3
  if [ -z "$header" ]; then
    code=$(curl -w '%{http_code}' -L -o "$local_file" "$source_url")
  else
    code=$(curl -w '%{http_code}' -L -H "$header" -o "$local_file" "$source_url")
  fi
  if [ "$code" != "200" ]; then
    log_debug "http_download_curl received HTTP status $code"
    return 1
  fi
  return 0
}
is_command() {
  command -v "$1" >/dev/null
}
http_download() {
  log_debug "http_download $2"
  if is_command curl; then
    http_download_curl "$@"
    return
  fi
  log_crit "http_download unable to find curl"
  return 1
}
http_copy() {
  tmp=$(mktemp)
  http_download "${tmp}" "$1" "$2" || return 1
  body=$(cat "$tmp")
  rm -f "${tmp}"
  echo "$body"
}

# Setup download name and url for datafuse releases
#
# @param $1: rustup target architecture
# @param $2: github release version
# @return <stdout>: the url for download
set_name_url() {
  local _arch=$1;
  local _version=$2; shift
  NAME=datafuse--${_arch}.tar.gz
  TARBALL=${NAME}
  TARBALL_URL=${GITHUB_DOWNLOAD}/${_version}/${TARBALL}
  echo "$TARBALL_URL"
}

set_name() {
  local _arch=$1;
  local _version=$2; shift
  NAME=datafuse--${_arch}.tar.gz
  echo "$NAME"
}

# Run a command that should never fail. If the command fails execution
# will immediately terminate with an error showing the failing
# command.
ensure() {
    if ! "$@"; then
        log_err "command failed: $*"
        exit 1
    fi
}
need_cmd() {
    if ! check_cmd "$1"; then
        log_err "need '$1' (command not found)"
        exit 1
    fi
}

check_cmd() {
    command -v "$1" > /dev/null 2>&1
}
# Prompts the user to add ~/.datafuse/bin to their PATH variable
path_hint() {
    log_info "💡 Please add '${HOME}/${BINDIR}/' to your PATH variable"
    log_info "   You can run the following to set your PATH on shell startup:"
    # shellcheck disable=SC2016
    log_info '   For bash: echo '\''export PATH="'"${HOME}"/"${BINDIR}"':${PATH}"'\'' >> ~/.bashrc'
    # shellcheck disable=SC2016
    log_info '   For zsh : echo '\''export PATH="'"${HOME}"/"${BINDIR}"':${PATH}"'\'' >> ~/.zshrc'
    log_info ""
    log_info "   To use fuse-query or fuse-store you'll need to restart your shell or run the following:"
    # shellcheck disable=SC2016
    log_info '   export PATH="'"${HOME}"/"${BINDIR}"':${PATH}"'
}
DATAFUSE_REPO=datafuselabs/datafuse
GITHUB_DOWNLOAD=https://github.com/${DATAFUSE_REPO}/releases/download

main(){
  local _status _target _version _url _name
  need_cmd curl
  need_cmd uname
  need_cmd mktemp
  need_cmd chmod
  need_cmd mkdir
  need_cmd mv
  need_cmd tar
  log_info "👏👏👏 Welcome to use datafuse!"
#   Detect architecture and ensure it's supported
  get_architecture || return 1
  local _arch="$RETVAL"
  assert_nz "$_arch"
  _target=$(assert_supported_architecture ${_arch})
  _status=$?
  if [ $_status -ne 0 ]; then
      # If this architecture is not supported, return error
      log_err "❌ Architecture ${_arch} is not supported."
      abort_prompt_issue
  fi
  log_info "😊 Your Architecture ${_arch} is supported"
  set_tag || return 1
  _version="$TAG"
  _name=$(set_name "$_target" "$_version" || return 1)
  _url=$(set_name_url "$_target" "$_version" || return 1)
  download_datafuse "$_name" "$_url" || return 1
  log_info "🎉 Install complete!"
  path_hint
}
parse_args "$@"
main