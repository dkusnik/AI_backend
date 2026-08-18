package pl.gov.nac.warc.config;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Enhanced override resolver with:
 * 1. Stage-only overrides (producer:)
 * 2. Warnings for unmatched override keys
 * 3. Introspection logs for applied overrides
 * 4. Hierarchical override precedence:
 *
 * 1. stage-only (producer)
 * 2. stage.module (producer.warc-jwarc)
 * 3. module-only (warc-jwarc)
 *
 * Dotted keys inside these maps are expanded and deep-merged.
 */
public final class OverrideResolver {

  private static final Logger log = LogManager.getLogger(OverrideResolver.class);
  private static final String MODULE_NAME = "OverrideResolver";

  private OverrideResolver() {
  }

  // =====================================================================
  // PUBLIC API
  // =====================================================================

  /**
   * Resolve overrides for a module using hierarchical precedence.
   *
   * @param stageName  e.g. "producer"
   * @param moduleName e.g. "warc-jwarc"
   * @param overrides  pipeline.overrides map
   * @return merged override map for this module
   */
  @SuppressWarnings("unchecked")
  public static Map<String, Object> resolveForModule(
      String stageName,
      String moduleName,
      Map<String, Object> overrides) {

    Map<String, Object> result = new LinkedHashMap<>();

    // --- Tier 3: Pipeline Overrides ---
    if (overrides != null && !overrides.isEmpty()) {
      String keyStageOnly = stageName; // producer
      String keyStageModule = stageName + "." + moduleName; // producer.warc-jwarc
      String keyModuleOnly = moduleName; // warc-jwarc

      // Order within Tier 3 (standard hierarchical)
      if (overrides.containsKey(keyStageOnly)) {
        logApply(keyStageOnly);
        deepMerge(result, (Map<String, Object>) overrides.get(keyStageOnly));
      }
      if (overrides.containsKey(keyStageModule)) {
        logApply(keyStageModule);
        deepMerge(result, (Map<String, Object>) overrides.get(keyStageModule));
      }
      if (overrides.containsKey(keyModuleOnly)) {
        logApply(keyModuleOnly);
        deepMerge(result, (Map<String, Object>) overrides.get(keyModuleOnly));
      }

      // --- Tier 4: Flat Dotted Keys ---
      // Handle flat keys like "producer.warc-jwarc.files" or "producer.files"
      String prefixStageModule = stageName + "." + moduleName + ".";
      String prefixStageOnly = stageName + ".";

      for (Map.Entry<String, Object> entry : overrides.entrySet()) {
        String key = entry.getKey();
        if (key.startsWith(prefixStageModule)) {
          String subKey = key.substring(prefixStageModule.length());
          deepMerge(result, expandDottedKey(subKey, entry.getValue()));
        } else if (key.startsWith(prefixStageOnly)) {
          // producer.files -> apply to any module in this stage
          String subKey = key.substring(prefixStageOnly.length());
          // Skip if it's the moduleName itself (handled above)
          if (!subKey.equals(moduleName)) {
            deepMerge(result, expandDottedKey(subKey, entry.getValue()));
          }
        }
      }

      for (String key : overrides.keySet()) {
        if (!key.equals(keyStageOnly)
            && !key.equals(keyStageModule)
            && !key.equals(keyModuleOnly)
            && key.startsWith(stageName + ".")) {
          log.debug("Override key '{}' considered for stage '{}' module '{}'",
              key, stageName, moduleName);
        }
      }
      // Log unmatched keys at INFO (expected during config application)
      for (String key : overrides.keySet()) {
        if (!key.equals(keyStageOnly)
            && !key.equals(keyStageModule)
            && !key.equals(keyModuleOnly)
            && key.startsWith(stageName + ".")) {
          log.info("Override key '{}' considered for stage '{}' module '{}'",
              key, stageName, moduleName);
        }
      }
    }

    return result;
  }

  /**
   * Tier 1 (CLI) overrides - Highest precedence.
   * Supports wildcards: producer.*.inputFile matches
   * producer.<any-module>.inputFile
   */
  public static void applyCliOverrides(
      String stageName,
      String moduleName,
      Map<String, Object> target) {
    Map<String, Object> cli = CliOverrides.get();
    if (cli.isEmpty())
      return;

    // Handle null moduleName: match stage-only prefix (e.g., "input.files")
    if (moduleName == null) {
      String prefixStageOnly = stageName + ".";
      for (Map.Entry<String, Object> e : cli.entrySet()) {
        String key = e.getKey();
        Object val = e.getValue();
        if (key.startsWith(prefixStageOnly)) {
          String subKey = key.substring(prefixStageOnly.length());
          log.info("Applying CLI override (stage alias): {}", key);
          // If value is already parsed (List), use it directly; otherwise parse
          Object parsedVal = (val instanceof List || val instanceof Boolean || val instanceof Integer)
              ? val : parseCliValue(val.toString());
          deepMerge(target, expandDottedKey(subKey, parsedVal));
        }
      }
      return;
    }

    String prefixStageModule = stageName + "." + moduleName + ".";
    String prefixModuleOnly = moduleName + ".";
    String prefixWildcard = stageName + ".*."; // e.g., producer.*.

    for (Map.Entry<String, Object> e : cli.entrySet()) {
      String key = e.getKey();
      Object val = e.getValue();

      // If value is already parsed (List, Boolean, Integer), use it directly; otherwise parse
      Object parsedVal = (val instanceof List || val instanceof Boolean || val instanceof Integer)
          ? val : parseCliValue(val.toString());

      if (key.startsWith(prefixStageModule)) {
        // Exact match: producer.warc-jwarc.inputFile
        String subKey = key.substring(prefixStageModule.length());
        log.info("Applying CLI override: {}", key);
        deepMerge(target, expandDottedKey(subKey, parsedVal));
      } else if (key.startsWith(prefixWildcard)) {
        // Wildcard match: producer.*.inputFile -> matches any producer module
        String subKey = key.substring(prefixWildcard.length());
        log.info("Applying CLI override (wildcard): {} -> {}.{}.{}",
            key, stageName, moduleName, subKey);
        deepMerge(target, expandDottedKey(subKey, parsedVal));
      } else if (key.startsWith(prefixModuleOnly)) {
        // Module-only match: warc-jwarc.inputFile
        String subKey = key.substring(prefixModuleOnly.length());
        log.info("Applying CLI override: {}", key);
        deepMerge(target, expandDottedKey(subKey, parsedVal));
      }
    }
  }

  private static void logApply(String key) {
    log.info("Applying override: {}", key);
  }

  // =====================================================================
  // DOTTED KEY EXPANSION
  // =====================================================================

  public static Map<String, Object> expandDottedKey(String dottedKey, Object value) {
    String[] parts = dottedKey.split("\\.");

    Map<String, Object> root = new LinkedHashMap<>();
    Map<String, Object> current = root;

    for (int i = 0; i < parts.length; i++) {
      String part = parts[i];

      if (i == parts.length - 1) {
        current.put(part, value);
      } else {
        Map<String, Object> next = new LinkedHashMap<>();
        current.put(part, next);
        current = next;
      }
    }

    return root;
  }

  // =====================================================================
  // SCALAR NORMALIZATION (for logical keys like mode/output)
  // =====================================================================

  /**
   * Keys that represent logical scalar values (NOT filesystem paths),
   * for which we should never keep a pathified value like /home/.../bytes.
   */
  private static final Set<String> SCALAR_KEYS = Set.of(
      "mode", // sequential | parallel
      "output", // bytes | native | universal
      "format",
      "type");

  private static boolean isScalarKey(String key) {
    if (key == null)
      return false;
    return SCALAR_KEYS.contains(key.toLowerCase(Locale.ROOT));
  }

  /**
   * If a scalar value looks like a path (/.../bytes), normalize it
   * to its last segment ("bytes").
   */
  private static Object normalizeScalar(String key, Object value) {
    if (!isScalarKey(key)) {
      return value;
    }
    if (!(value instanceof String s)) {
      return value;
    }

    s = s.trim();
    if (s.isEmpty()) {
      return s;
    }

    // Strip trailing separators
    while (s.endsWith("/") || s.endsWith("\\")) {
      s = s.substring(0, s.length() - 1);
    }

    int slash = Math.max(s.lastIndexOf('/'), s.lastIndexOf('\\'));
    if (slash >= 0 && slash + 1 < s.length()) {
      return s.substring(slash + 1).trim();
    }
    return s;
  }

  // =====================================================================
  // DEEP MERGE LOGIC
  // =====================================================================

  @SuppressWarnings("unchecked")
  public static void deepMerge(Map<String, Object> target, Map<String, Object> source) {
    for (Map.Entry<String, Object> e : source.entrySet()) {
      String key = e.getKey();
      Object srcVal = e.getValue();

      // Normalize scalar logical keys (mode/output/etc.)
      srcVal = normalizeScalar(key, srcVal);

      if (!target.containsKey(key)) {
        target.put(key, srcVal);
        continue;
      }

      Object tgtVal = target.get(key);

      if (tgtVal instanceof Map && srcVal instanceof Map) {
        deepMerge((Map<String, Object>) tgtVal, (Map<String, Object>) srcVal);
      } else {
        target.put(key, srcVal);
      }
    }
  }

  // =====================================================================
  // CLI OVERRIDES
  // =====================================================================

  public static final class CliOverrides {

    private static final Map<String, Object> OVERRIDES = new LinkedHashMap<>();

    private CliOverrides() {
    }

    public static void load(String[] args) {
      OVERRIDES.clear();

      for (String arg : args) {
        if (!arg.startsWith("--"))
          continue;

        String trimmed = arg.substring(2);
        int idx = trimmed.indexOf('=');
        String key;
        String value;
        if (idx < 0) {
          key = trimmed;
          value = "true";
        } else {
          key = trimmed.substring(0, idx);
          value = trimmed.substring(idx + 1);
        }

        OVERRIDES.put(key, value);
      }
    }

    public static Map<String, Object> get() {
      return OVERRIDES;
    }
  }

  public static Object parseCliValue(String raw) {
    if (raw == null)
      return null;

    if (raw.equalsIgnoreCase("true"))
      return Boolean.TRUE;
    if (raw.equalsIgnoreCase("false"))
      return Boolean.FALSE;

    try {
      return Integer.parseInt(raw);
    } catch (Exception _) {
    }
    try {
      return Double.parseDouble(raw);
    } catch (Exception _) {
    }

    if (raw.contains(",")) {
      String[] parts = raw.split(",");
      List<String> list = new ArrayList<>();
      for (String p : parts)
        list.add(p.trim());
      return list;
    }

    return raw;
  }
}
