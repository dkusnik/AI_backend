# Traversal-order contract fixture

Run `materialize.sh DESTINATION` to create the complete fixture. The script copies the tracked
portable base and creates the whitespace-bearing filenames at test time; tracking those names under
`src/main/dist` would break GNU Make's whitespace-delimited prerequisite expansion.

The resulting `valid-root/` exercises canonical, recursive discovery. `expected-order.hex` is the
normative list of root-relative UTF-8 path bytes after canonicalization, duplicate removal, and
`LC_ALL=C` bytewise sorting. Each non-comment line is one complete path encoded as lowercase
hexadecimal; the expected order is stored here and must never be generated using the test runner's
locale.

The materializer copies the portable `.wet` templates to supported `.wet.gz` names.
`valid-root/duplicate.wet.gz` is a symlink to `nested/02.wet.gz`. It resolves to a canonical
duplicate and therefore does not add an entry. The tracked legacy `.wet` templates and
`valid-root/ignore.txt` are unrelated and ignored.

`escape-root/escape.wet.gz` is a symlink to `outside.wet.gz`, outside its positional root. Discovery
of `escape-root/` must fail before any artifact is processed.

The materialized fixture also contains supported names with a space, a tab, a newline, and
non-ASCII UTF-8.
