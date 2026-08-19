#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 1 ]]; then
    echo "usage: materialize.sh DESTINATION" >&2
    exit 2
fi

destination=$1
if [[ -e "$destination" || -L "$destination" ]]; then
    echo "materialize.sh: destination already exists: $destination" >&2
    exit 2
fi

fixture_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
mkdir -p -- "$destination"
cp -a -- "$fixture_dir/valid-root" "$destination/"
cp -a -- "$fixture_dir/escape-root" "$destination/"
cp -a -- "$fixture_dir/outside.wet" "$destination/outside.wet.gz"

# The tracked portable base uses whitespace-safe template names. Materialize only the
# currently supported .wet.gz contract; the old .wet templates and symlinks remain ignored.
cp -a -- "$fixture_dir/valid-root/A.wet" "$destination/valid-root/A.wet.gz"
cp -a -- "$fixture_dir/valid-root/a.wet" "$destination/valid-root/a.wet.gz"
cp -a -- "$fixture_dir/valid-root/nested/10.wet" "$destination/valid-root/nested/10.wet.gz"
cp -a -- "$fixture_dir/valid-root/utf8-ą.wet" "$destination/valid-root/utf8-ą.wet.gz"
ln -s -- nested/02.wet.gz "$destination/valid-root/duplicate.wet.gz"
ln -s -- ../outside.wet.gz "$destination/escape-root/escape.wet.gz"

# Literal hostile names are created at test time. Keeping whitespace-bearing names in src/main/dist
# would make GNU Make split its prerequisite list before the harness can test NUL-safe traversal.
touch -- "$destination/valid-root/space name.wet.gz"
touch -- "$destination/valid-root/"$'tab\tname.wet.gz'
touch -- "$destination/valid-root/"$'line\nname.wet.gz'
