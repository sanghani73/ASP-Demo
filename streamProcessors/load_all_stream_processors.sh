#!/usr/bin/env bash

# Runs mongosh for each stream processor creation script in a directory.
# Connection string is read from MONGODB_URI env var or a properties file.
#
# Usage:
#  Set MONGODB_URI in your environment:
#        export MONGODB_URI="mongodb://user:pass@host:port/?options"
#  or create ./mongodb.properties with:
#        MONGODB_URI=mongodb://user:pass@host:port/?options
#  and run: sh ./load_all_stream_processors.sh

set -u

PROPERTIES_FILE="${MONGODB_PROPERTIES_FILE:-./mongodb.properties}"

# Resolve MONGODB_URI: prefer env var, else read from properties file
if [ -z "${MONGODB_URI:-}" ]; then
  if [ -f "$PROPERTIES_FILE" ]; then
    # Read the last occurrence of MONGODB_URI=... and extract the value (handles '=' inside URI)
    line="$(grep -E '^[[:space:]]*MONGODB_URI[[:space:]]*=' "$PROPERTIES_FILE" | tail -n1 || true)"
    if [ -n "$line" ]; then
      MONGODB_URI="$(printf '%s' "$line" | sed -E 's/^[[:space:]]*MONGODB_URI[[:space:]]*=[[:space:]]*//' | sed -E 's/^[\"\x27]?//; s/[\"\x27]?$//' | sed -E 's/[[:space:]]+$//')"
    fi
  fi
fi

if [ -z "${MONGODB_URI:-}" ]; then
  echo "Error: MONGODB_URI not set and no usable properties file found."
  echo "Set MONGODB_URI or create a properties file (e.g., ./mongodb.properties) with:"
  echo "  MONGODB_URI=mongodb://user:pass@host:port/?options"
  exit 1
fi

# Ensure mongosh is available
if ! command -v mongosh >/dev/null 2>&1; then
  echo "Error: mongosh not found in PATH. Please install mongosh and try again."
  exit 1
fi

files=( ./create*.js )

if [ ${#files[@]} -eq 0 ]; then
  echo "No files found matching: ./create*.js"
  exit 0
fi

echo "Processing ${#files[@]} file(s)"
fail_count=0

for file in "${files[@]}"; do
  echo "------------------------------------------------------------"
  echo "Running mongosh for: $file"
  # echo mongosh "$MONGODB_URI" $TLS_FLAG --file "$file"
  mongosh "$MONGODB_URI" --tls --file "$file"
  status=$?
  if [ $status -ne 0 ]; then
    echo "Error: mongosh failed for '$file' (exit code $status)."
    fail_count=$((fail_count + 1))
  else
    echo "Success: $file"
  fi
done

echo "------------------------------------------------------------"
echo "Completed. Total files: ${#files[@]} | Failures: $fail_count"
if [ $fail_count -ne 0 ]; then
  exit 1
fi
exit 0