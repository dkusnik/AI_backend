# readability-jni — Rust JNI/FFI bridge for native text extraction.
# Produces target/release/libreadability_jni.so, consumed by the warc2es
# Maven build (see ../../pom.xml prepare-out-distribution).

SO := target/release/libreadability_jni.so

.PHONY: default all abi clean clippy fmt test unit

default: all

all:
	cargo build --release
	@ls -la $(SO)

fmt:
	cargo fmt --check

clippy:
	cargo clippy --release -- -D warnings

unit:
	cargo test --release

abi: all
	@actual=$$(mktemp); expected=$$(mktemp); \
	trap 'rm -f "$$actual" "$$expected"' EXIT; \
	printf '%s\n' \
		'T readability_extract' \
		'T readability_extract_full' \
		'T readability_extract_full_bytes' \
		'T readability_free_full_result' \
		'T readability_free_result' > "$$expected"; \
	nm -D --defined-only $(SO) | awk '{print $$2, $$3}' | LC_ALL=C sort > "$$actual"; \
	diff -u "$$expected" "$$actual"

test: fmt clippy unit abi

clean:
	cargo clean
