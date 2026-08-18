package pl.gov.nac.warc.records.warc;

import pl.gov.nac.warc.records.RecordInMemory;

/**
 * A WARC record that has already been compressed into its own GZIP member.
 * Contains the compressed bytes and metadata for CDX generation.
 * This allows shifting compression work to parallel worker threads.
 */
public record RecordCompressed(
    byte[] compressedBytes,
    String targetUri,
    String warcDate,
    String contentType,
    String warcType,
    String digest,
    String provenance,
    String source) implements RecordInMemory {

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (!(o instanceof RecordCompressed that))
      return false;
    return java.util.Arrays.equals(compressedBytes, that.compressedBytes) &&
        java.util.Objects.equals(targetUri, that.targetUri) &&
        java.util.Objects.equals(warcDate, that.warcDate) &&
        java.util.Objects.equals(contentType, that.contentType) &&
        java.util.Objects.equals(warcType, that.warcType) &&
        java.util.Objects.equals(digest, that.digest) &&
        java.util.Objects.equals(provenance, that.provenance) &&
        java.util.Objects.equals(source, that.source);
  }

  @Override
  public int hashCode() {
    int result = java.util.Objects.hash(targetUri, warcDate, contentType, warcType, digest, provenance, source);
    result = 31 * result + java.util.Arrays.hashCode(compressedBytes);
    return result;
  }

  @Override
  public String toString() {
    return "RecordCompressed[" +
        "compressedBytes=" + (compressedBytes == null ? "null" : compressedBytes.length + " bytes") +
        ", targetUri=" + targetUri +
        ", warcDate=" + warcDate +
        ", contentType=" + contentType +
        ", warcType=" + warcType +
        ", digest=" + digest +
        ", provenance=" + provenance +
        ", source=" + source +
        ']';
  }

  @Override
  public String typeName() {
    return "RecordCompressed";
  }

  @Override
  public long actualDataSize() {
    return compressedBytes != null ? compressedBytes.length : 0;
  }
}
