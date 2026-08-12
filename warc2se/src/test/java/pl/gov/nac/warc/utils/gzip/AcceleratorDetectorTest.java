package pl.gov.nac.warc.utils.gzip;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;

import org.junit.jupiter.api.Test;

class AcceleratorDetectorTest {

  @Test
  void resolveIsaLLibraryPrefersSystemPropertyOverride() {
    AcceleratorDetector.IsaLLibraryResolution resolution = AcceleratorDetector.resolveIsaLLibrary(
        "/custom/libisal.so.2",
        "/env/libisal.so.2",
        path -> Set.of("/custom/libisal.so.2", "/env/libisal.so.2").contains(path));

    assertTrue(resolution.available());
    assertEquals("/custom/libisal.so.2", resolution.resolvedPath());
    assertEquals("system-property", resolution.resolutionMode());
  }

  @Test
  void resolveIsaLLibraryUsesEnvironmentOverrideWhenPropertyMissing() {
    AcceleratorDetector.IsaLLibraryResolution resolution = AcceleratorDetector.resolveIsaLLibrary(
        null,
        "/env/libisal.so.2",
        "/env/libisal.so.2"::equals);

    assertTrue(resolution.available());
    assertEquals("/env/libisal.so.2", resolution.resolvedPath());
    assertEquals("environment", resolution.resolutionMode());
  }

  @Test
  void resolveIsaLLibraryUsesWellKnownPathWhenNoOverridesProvided() {
    AcceleratorDetector.IsaLLibraryResolution resolution = AcceleratorDetector.resolveIsaLLibrary(
        null,
        null,
        "/usr/local/lib/libisal.so.2"::equals);

    assertTrue(resolution.available());
    assertEquals("/usr/local/lib/libisal.so.2", resolution.resolvedPath());
    assertEquals("well-known-path", resolution.resolutionMode());
  }

  @Test
  void resolveIsaLLibraryFailsFastForMissingConfiguredPath() {
    AcceleratorDetector.IsaLLibraryResolution resolution = AcceleratorDetector.resolveIsaLLibrary(
        "/missing/libisal.so.2",
        null,
        path -> false);

    assertFalse(resolution.available());
    assertEquals("system-property", resolution.resolutionMode());
    assertTrue(resolution.detail().contains("/missing/libisal.so.2"));
  }

  @Test
  void resolveIsaLLibraryReportsNotFoundWhenNoCandidateExists() {
    AcceleratorDetector.IsaLLibraryResolution resolution = AcceleratorDetector.resolveIsaLLibrary(
        null,
        null,
        path -> false);

    assertFalse(resolution.available());
    assertEquals("not-found", resolution.resolutionMode());
    assertTrue(resolution.detail().contains("No ISA-L library found"));
  }
}
