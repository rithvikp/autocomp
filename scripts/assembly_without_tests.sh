#! /usr/bin/env bash

set -euo pipefail

main() {
    sbt "project frankenpaxosJVM" "set test in assembly := {}"  frankenpaxosJVM/assembly
}

main "$@"