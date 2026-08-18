package pl.gov.nac.warc.config;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * PathResolver
 *
 * Expands:
 * - ${ENV} and ${ENV:-default}
 * - ~/home expansion
 * - relative → absolute normalization
 * - recursive resolution inside maps/lists
 *
 * NEW (scoped):
 * - Only keys that represent filesystem paths are resolved:
 * file, files, input, output, outputFile, directory, dir, path.
 *
 * - If a resolved path does not exist on filesystem,
 * attempt to load it from classpath using the SAME relative path.
 *
 * Example:
 * config: "defaults/pipeline.yaml"
 * classpath lookup: "/defaults/pipeline.yaml"
 *
 * Example:
 * config: "pipeline.yaml"
 * classpath lookup: "/pipeline.yaml"
 */
public final class PathResolver {

    private static final Logger log = LogManager.getLogger(PathResolver.class);

    private PathResolver() {
    }

    // Keys that represent filesystem paths in config
    private static final Set<String> PATH_KEYS = Set.of(
            "file", "files", "input", "output", "outputfile",
            "directory", "dir", "path", "rocksdb-path");

    private static boolean isPathKey(String key) {
        if (key == null)
            return false;
        return PATH_KEYS.contains(key.toLowerCase(Locale.ROOT));
    }

    // =====================================================================
    // PUBLIC API
    // =====================================================================

    @SuppressWarnings("unchecked")
    public static void resolvePaths(Map<String, Object> cfg) {
        if (cfg == null || cfg.isEmpty())
            return;

        for (Map.Entry<String, Object> e : cfg.entrySet()) {
            String key = e.getKey();
            Object v = e.getValue();

            if (v instanceof String s) {
                // Only resolve if this key is known to represent a path
                if (isPathKey(key)) {
                    e.setValue(resolveString(s));
                }
            } else if (v instanceof Map) {
                resolvePaths((Map<String, Object>) v);
            } else if (v instanceof List) {
                resolveList(key, (List<Object>) v);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static void resolveList(String parentKey, List<Object> list) {
        for (int i = 0; i < list.size(); i++) {
            Object v = list.get(i);

            if (v instanceof String s) {
                // Elements of a list are treated as paths only if the parent key is a path key
                if (isPathKey(parentKey)) {
                    list.set(i, resolveString(s));
                }
            } else if (v instanceof Map) {
                resolvePaths((Map<String, Object>) v);
            } else if (v instanceof List) {
                resolveList(parentKey, (List<Object>) v);
            }
        }
    }

    // =====================================================================
    // STRING RESOLUTION
    // =====================================================================

    private static String resolveString(String raw) {
        if (raw == null || raw.isEmpty())
            return raw;

        String s = expandEnv(raw);
        s = expandTilde(s);

        // Normalize relative filesystem path to absolute
        if (!s.startsWith("/")) {
            s = normalize(s);
        }

        // Fallback to classpath
        s = fallbackToClasspath(s);

        return s;
    }

    // =====================================================================
    // ENV EXPANSION
    // =====================================================================

    private static String expandEnv(String raw) {
        StringBuilder sb = new StringBuilder();
        int i = 0;

        while (i < raw.length()) {
            int start = raw.indexOf("${", i);
            if (start < 0) {
                sb.append(raw.substring(i));
                return sb.toString();
            }

            sb.append(raw, i, start);

            int end = raw.indexOf("}", start);
            if (end < 0) {
                sb.append(raw.substring(start));
                return sb.toString();
            }

            String expr = raw.substring(start + 2, end);
            sb.append(resolveEnvExpression(expr));

            i = end + 1;
        }

        return sb.toString();
    }

    private static String resolveEnvExpression(String expr) {
        if (expr.contains(":-")) {
            String[] parts = expr.split(":-", 2);
            String value = System.getenv(parts[0]);
            return (value == null || value.isEmpty()) ? parts[1] : value;
        }
        String value = System.getenv(expr);
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("Environment variable is not set: " + expr);
        }
        return value;
    }

    // =====================================================================
    // TILDE EXPANSION
    // =====================================================================

    private static String expandTilde(String raw) {
        if (raw.startsWith("~/")) {
            return System.getProperty("user.home") + raw.substring(1);
        }
        return raw;
    }

    // =====================================================================
    // NORMALIZATION
    // =====================================================================

    private static String normalize(String raw) {
        try {
            return Path.of(raw).toAbsolutePath().normalize().toString();
        } catch (Exception _) {
            log.debug("Path normalization failed for '{}', returning raw path", raw);
            return raw;
        }
    }

    // =====================================================================
    // CLASSPATH FALLBACK
    // =====================================================================

    private static String fallbackToClasspath(String path) {

        Path p = Path.of(path);

        // If file exists on filesystem, use it
        if (Files.exists(p))
            return path;

        // Try full relative path as classpath resource
        // Convert Windows path delimiters to forward slashes for classpath
        // compatibility
        String cpPath = "/" + p.toString().replace("\\", "/"); // NOSONAR - cross-platform path handling

        try (InputStream in = PathResolver.class.getResourceAsStream(cpPath)) {
            if (in != null) {
                return cpPath; // Found inside JAR or IDE classpath
            }
        } catch (Exception _) { // NOSONAR - fallback: try next location
            log.debug("Classpath fallback lookup failed for '{}'", cpPath);
        }

        // Try only the filename (last resort)
        String fileOnly = "/" + p.getFileName().toString();
        try (InputStream in = PathResolver.class.getResourceAsStream(fileOnly)) {
            if (in != null) {
                return fileOnly;
            }
        } catch (Exception _) { // NOSONAR - fallback: return original path
            log.debug("Classpath fallback filename lookup failed for '{}'", fileOnly);
        }

        return path;
    }
}
