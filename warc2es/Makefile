.PHONY: default all native java build check-isal clean clean-java clean-native test test-rebuild runtime-preservation-check release release-check

default: all

# ---------------------------------------------------------------------------
# Dependency model (A0-001 / AR-006)
#
# `test` previously had no prerequisites and decided whether to rebuild with a
# shell heuristic that inspected only src/main/java. A change to src/native/src or
# src/main/dist therefore rebuilt nothing, and the suite reported green against
# stale artifacts. Cargo rebuilding the .so was also insufficient on its own:
# Maven has to restage it into target/dist/native/ and out/app/native/.
#
# The chain below is expressed as ordinary prerequisites, so a Rust, Java, shell
# or pom change reaches the artifacts the tests actually load.
# ---------------------------------------------------------------------------

NATIVE_SO  := src/native/target/release/libreadability_jni.so
NATIVE_SRC := $(shell find src/native/src -name '*.rs' 2>/dev/null) src/native/Cargo.toml src/native/Cargo.lock src/native/Makefile
JAR        := target/dist/lib/pipeline.jar
JAVA_SRC   := $(shell find src/main/java -name '*.java' 2>/dev/null)
DIST_SRC   := $(shell find src/main/dist -type f 2>/dev/null)
RES_SRC    := $(shell find src/main/resources -type f 2>/dev/null)
RELEASE_STAGE   := target/release-root
RELEASE_ARCHIVE := target/warc2es-linux.tar.gz
RELEASE_CHECK_ROOT := target/release-check-root
ISAL_DEFAULT_PATHS ?= \
	/usr/lib/x86_64-linux-gnu/libisal.so.2 \
	/usr/lib/libisal.so.2 \
	/usr/local/lib/libisal.so.2 \
	/lib/x86_64-linux-gnu/libisal.so.2

# Full build: native library first (Maven fails fast if the .so is missing),
# then the Java package, which assembles target/dist/ and the out/ MVP runtime.
all: build

build: check-isal $(JAR)

check-isal:
	@configured_path="$${WARC_ISAL_LIBRARY:-}"; \
	if [ -n "$$configured_path" ] && [ -f "$$configured_path" ]; then \
		exit 0; \
	fi; \
	for candidate in $(ISAL_DEFAULT_PATHS); do \
		if [ -f "$$candidate" ]; then \
			exit 0; \
		fi; \
	done; \
	printf '%s\n' \
		'WARNING: ISA-L runtime library libisal.so.2 was not found.' \
		'Install libisal2 or set WARC_ISAL_LIBRARY to its absolute path; ISA-L tests will be skipped.' >&2

native: $(NATIVE_SO)

$(NATIVE_SO): $(NATIVE_SRC)
	$(MAKE) -C src/native

java: check-isal $(JAR)

$(JAR): $(NATIVE_SO) $(JAVA_SRC) $(DIST_SRC) $(RES_SRC) pom.xml
	mvn clean package

clean: clean-java clean-native

clean-java:
	rm -rf dist target

clean-native:
	$(MAKE) -C src/native clean

# Build output lives in target/dist/ (root dist/ is no longer produced).
test: build
	$(MAKE) -C src/native test
	./target/dist/bin/test-cli fast

# Force a full rebuild, then the fast suite. Referenced by README.md for
# release-candidate validation.
test-rebuild: clean
	$(MAKE) test

# Destructive only to generated build output. The probe injects uniquely named
# runtime sentinels, performs make clean + make, verifies their hashes, and
# removes only the sentinels it created.
runtime-preservation-check:
	bash src/main/dist/testing/scripts/fast/s0-packaging-freshness.sh --preservation-rebuild

# Build a release from an explicit immutable-artifact allowlist. Never archive
# out/: it may contain a deployment profile, input WARCs, derivatives or locks.
release: build
	rm -rf -- "$(RELEASE_STAGE)"
	mkdir -p "$(RELEASE_STAGE)/app/bin" \
		"$(RELEASE_STAGE)/app/conf/elasticsearch/templates" \
		"$(RELEASE_STAGE)/app/lib/scripts" \
		"$(RELEASE_STAGE)/app/native" \
		"$(RELEASE_STAGE)/in" \
		"$(RELEASE_STAGE)/wet" \
		"$(RELEASE_STAGE)/doet" \
		"$(RELEASE_STAGE)/all"
	install -m 0644 target/dist/README.md "$(RELEASE_STAGE)/README.md"
	install -m 0644 target/dist/.profile.example "$(RELEASE_STAGE)/.profile.example"
	install -m 0755 \
		target/dist/warc2wet.sh \
		target/dist/wet-merge.sh \
		target/dist/es-upsert.sh \
		target/dist/es-upsert-all.sh \
		target/dist/es-delete.sh \
		target/dist/es-reinit.sh \
		"$(RELEASE_STAGE)/"
	install -m 0755 target/dist/bin/warc-cli target/dist/bin/es-cli \
		"$(RELEASE_STAGE)/app/bin/"
	install -m 0644 src/main/resources/config-out.yaml \
		"$(RELEASE_STAGE)/app/conf/config.yaml"
	install -m 0644 target/dist/conf/elasticsearch/templates/*.json \
		"$(RELEASE_STAGE)/app/conf/elasticsearch/templates/"
	install -m 0644 target/dist/lib/*.jar "$(RELEASE_STAGE)/app/lib/"
	install -m 0755 target/dist/lib/scripts/pipeline-lib \
		target/dist/lib/scripts/runtime-lib.sh \
		"$(RELEASE_STAGE)/app/lib/scripts/"
	install -m 0755 target/dist/native/libreadability_jni.so \
		"$(RELEASE_STAGE)/app/native/"
	tar -C "$(RELEASE_STAGE)" -czf "$(CURDIR)/$(RELEASE_ARCHIVE)" .

# Refuse any archive member outside the release allowlist and verify the
# executable modes that the operator entrypoints require.
release-check: release
	@set -eu; \
	list=target/release-members.txt; \
	tar -tzf "$(RELEASE_ARCHIVE)" > "$$list"; \
	while IFS= read -r member; do \
		case "$$member" in \
			./) continue ;; \
			./*/) path=$${member#./}; path=$${path%/}; \
				case "$$path" in \
					app|app/bin|app/conf|app/conf/elasticsearch|app/conf/elasticsearch/templates|app/lib|app/lib/scripts|app/native|in|wet|doet|all) ;; \
					*) echo "Unexpected release directory: $$member" >&2; exit 1 ;; \
				esac ;; \
			*) path=$${member#./}; \
				case "$$path" in \
					README.md|.profile.example|warc2wet.sh|wet-merge.sh|es-upsert.sh|es-upsert-all.sh|es-delete.sh|es-reinit.sh|app/bin/warc-cli|app/bin/es-cli|app/conf/config.yaml|app/conf/elasticsearch/templates/*.json|app/lib/*.jar|app/lib/scripts/pipeline-lib|app/lib/scripts/runtime-lib.sh|app/native/libreadability_jni.so) ;; \
					*) echo "Unexpected release file: $$member" >&2; exit 1 ;; \
				esac ;; \
		esac; \
	done < "$$list"; \
	rm -rf -- "$(RELEASE_CHECK_ROOT)"; \
	mkdir -p "$(RELEASE_CHECK_ROOT)"; \
	tar -xzf "$(RELEASE_ARCHIVE)" -C "$(RELEASE_CHECK_ROOT)"; \
	for required in README.md .profile.example warc2wet.sh wet-merge.sh \
		es-upsert.sh es-upsert-all.sh es-delete.sh es-reinit.sh \
		app/bin/warc-cli app/bin/es-cli app/conf/config.yaml \
		app/lib/pipeline.jar app/lib/scripts/pipeline-lib \
		app/lib/scripts/runtime-lib.sh app/native/libreadability_jni.so; do \
		test -e "$(RELEASE_CHECK_ROOT)/$$required" || { echo "Missing release member: $$required" >&2; exit 1; }; \
	done; \
	for executable in warc2wet.sh wet-merge.sh es-upsert.sh es-upsert-all.sh \
		es-delete.sh es-reinit.sh app/bin/warc-cli app/bin/es-cli \
		app/lib/scripts/pipeline-lib app/lib/scripts/runtime-lib.sh; do \
		test -x "$(RELEASE_CHECK_ROOT)/$$executable" || { echo "Release member is not executable: $$executable" >&2; exit 1; }; \
	done; \
	test ! -e "$(RELEASE_CHECK_ROOT)/.profile"; \
	test ! -e "$(RELEASE_CHECK_ROOT)/testing"; \
	test ! -e "$(RELEASE_CHECK_ROOT)/app/tmp"; \
	test ! -e "$(RELEASE_CHECK_ROOT)/app/var"; \
	test ! -e "$(RELEASE_CHECK_ROOT)/var"; \
	test ! -e "$(RELEASE_CHECK_ROOT)/log"; \
	test ! -e "$(RELEASE_CHECK_ROOT)/app/lib/scripts/pipeline-direct"; \
	for data_dir in in wet doet all; do \
		test -z "$$(find "$(RELEASE_CHECK_ROOT)/$$data_dir" -type f -print -quit)" || { echo "Release contains runtime data under $$data_dir/" >&2; exit 1; }; \
	done; \
	for command in warc2wet.sh wet-merge.sh es-upsert.sh es-upsert-all.sh es-delete.sh es-reinit.sh; do \
		"$(RELEASE_CHECK_ROOT)/$$command" --help >/dev/null; \
	done; \
	echo "Release allowlist and executable modes verified: $(RELEASE_ARCHIVE)"
