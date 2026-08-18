package pl.gov.nac.warc.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pl.gov.nac.warc.Pipeline;

/**
 * Build metadata helper used by CLI version reporting.
 */
public final class BuildInfo {
  private static final Logger log = LogManager.getLogger(BuildInfo.class);

  private static final String UNKNOWN_VERSION = "(development build)";
  private static final String MAVEN_POM_PROPERTIES = "/META-INF/maven/pl.gov.nac.warc/pipeline/pom.properties";

  private BuildInfo() {
  }

  public static String version() {
    String implVersion = Pipeline.class.getPackage().getImplementationVersion();
    if (implVersion != null && !implVersion.isBlank()) {
      return implVersion;
    }

    try (InputStream in = Pipeline.class.getResourceAsStream(MAVEN_POM_PROPERTIES)) {
      if (in == null) {
        return UNKNOWN_VERSION;
      }
      Properties properties = new Properties();
      properties.load(in);
      String version = properties.getProperty("version");
      return (version == null || version.isBlank()) ? UNKNOWN_VERSION : version;
    } catch (IOException e) {
      log.debug("Failed to load build metadata from {}", MAVEN_POM_PROPERTIES, e);
      return UNKNOWN_VERSION;
    }
  }
}
