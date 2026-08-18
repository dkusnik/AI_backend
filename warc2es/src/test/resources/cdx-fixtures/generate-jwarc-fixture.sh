#!/usr/bin/env bash
set -euo pipefail

OUT_DIR="$(cd "$(dirname "$0")" && pwd)"
OUT_FILE="$OUT_DIR/sample-jwarc.warc"
JAVA_FILE="/tmp/CdxFixtureGenerator.java"
CLASS_DIR="/tmp"
JWARC_JAR="$HOME/.m2/repository/org/netpreserve/jwarc/0.33.0/jwarc-0.33.0.jar"

cat > "$JAVA_FILE" <<'JAVA'
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.UUID;

import org.netpreserve.jwarc.HttpResponse;
import org.netpreserve.jwarc.MediaType;
import org.netpreserve.jwarc.WarcResponse;
import org.netpreserve.jwarc.WarcWriter;

public class CdxFixtureGenerator {
  public static void main(String[] args) throws Exception {
    Path out = Path.of(args[0]);
    try (OutputStream os = Files.newOutputStream(out); WarcWriter writer = new WarcWriter(os)) {
      HttpResponse http1 = new HttpResponse.Builder(200, "OK")
          .addHeader("Content-Type", "text/plain")
          .body(MediaType.parse("text/plain"), "hello-a".getBytes())
          .build();
      WarcResponse rec1 = new WarcResponse.Builder(URI.create("http://www.example.com/path"))
          .recordId(UUID.fromString("11111111-1111-1111-1111-111111111111"))
          .date(Instant.parse("2026-01-01T00:00:00Z"))
          .body(http1)
          .build();
      writer.write(rec1);

      HttpResponse http2 = new HttpResponse.Builder(200, "OK")
          .addHeader("Content-Type", "text/plain")
          .body(MediaType.parse("text/plain"), "hello-b".getBytes())
          .build();
      WarcResponse rec2 = new WarcResponse.Builder(URI.create("http://www.example.com/other"))
          .recordId(UUID.fromString("22222222-2222-2222-2222-222222222222"))
          .date(Instant.parse("2026-01-01T00:00:01Z"))
          .body(http2)
          .build();
      writer.write(rec2);
    }
  }
}
JAVA

javac -cp "$JWARC_JAR" -d "$CLASS_DIR" "$JAVA_FILE"
java -cp "$CLASS_DIR:$JWARC_JAR" CdxFixtureGenerator "$OUT_FILE"
sha256sum "$OUT_FILE" > "$OUT_DIR/sample-jwarc.warc.sha256"

echo "Generated: $OUT_FILE"
