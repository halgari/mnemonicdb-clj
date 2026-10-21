#!/usr/bin/env bash
# Aggregate JFR execution samples into a top-frames table.
#   bench/jfr-top.sh recording.jfr [N=15] [self|incl]
# self: the top (leaf) frame of each sample; incl: every distinct frame in the stack,
# counted once per sample. Needs only the JDK's `jfr` tool.
set -eu
file=${1:?usage: jfr-top.sh recording.jfr [N] [self|incl]}
n=${2:-15}
mode=${3:-self}
jfr print --events jdk.ExecutionSample --stack-depth 64 "$file" | awk -v mode="$mode" '
  /jdk.ExecutionSample/ { inst=1; first=1; delete seen; total++; next }
  inst && /^[ \t]+[a-zA-Z_$].*line:/ {
    f=$0; sub(/^[ \t]+/, "", f); sub(/\(.*/, "", f)
    if (mode=="self") { if (first) { c[f]++; first=0 } }
    else if (!(f in seen)) { c[f]++; seen[f]=1 }
    next
  }
  /^\}/ { inst=0 }
  END { for (f in c) printf "%6.1f%% %7d  %s\n", 100*c[f]/total, c[f], f; printf "%s %d\n", "#samples", total > "/dev/stderr" }
' | sort -rn | head -n "$n"
