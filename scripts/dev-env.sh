#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
java_home="$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home"

if [[ ! -x "${java_home}/bin/java" ]]; then
  printf 'Java 17 was not found at %s\n' "${java_home}" >&2
  return 1 2>/dev/null || exit 1
fi

export JAVA_HOME="${java_home}"
export PATH="${JAVA_HOME}/bin:${PATH}"
export PYSPARK_PYTHON="${project_root}/.venv/bin/python"

unset project_root java_home
