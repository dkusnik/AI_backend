package pl.gov.nac.warc.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/**
 * Parses command-line arguments based on definitions in config.yaml.
 * Supports both flag-based options (e.g., -f, --file) and positional arguments
 * (unnamed).
 *
 * Arg format in YAML:
 * args:
 * - ["-h", "--help", "boolean", "global.help"] # Flag
 * - [0, "string", "output"] # Positional index 0
 * - ["string[]", "inputFiles"] # Remainder (all other non-flag args)
 */
public final class ArgParser {

  private static final String TYPE_BOOLEAN = "boolean";
  private static final String RESULT_FORMAT_OPTION = "--result-format";
  private static final Set<String> ALLOWED_BARE_OVERRIDES = Set.of("producer", "consumer");
  private static final Set<String> ALLOWED_OVERRIDE_PREFIXES = Set.of(
      "global", "engine", "producer", "processor", "consumer", "checker", "before", "after", "input", "output");

  private ArgParser() {
  }

  public record ArgDef(
      String shortOpt,
      String longOpt,
      Integer index,
      boolean isRemainder,
      String type,
      String targetPath) {
  }

  /** Built-in operator protocol option, independent of the selected YAML profile. */
  public static ArgDef resultFormatDefinition() {
    return new ArgDef("", RESULT_FORMAT_OPTION, null, false, "string", "global.resultFormat");
  }

  /**
   * Detect result mode before configuration and Log4j are initialized. Full
   * validation still belongs to {@link Config}; this method only selects the
   * console stream early enough to keep JSON stdout clean on startup failures.
   */
  public static String detectResultFormat(String[] args) {
    String result = "human";
    for (int i = 0; i < args.length; i++) {
      String arg = args[i];
      if ("--".equals(arg)) {
        break;
      }
      if (arg.startsWith(RESULT_FORMAT_OPTION + "=")) {
        result = arg.substring((RESULT_FORMAT_OPTION + "=").length());
      } else if (RESULT_FORMAT_OPTION.equals(arg) && i + 1 < args.length) {
        result = args[++i];
      }
    }
    return result;
  }

  /**
   * Parse arguments according to the given definitions.
   * Returns a map of targetPath -> parsed value.
   *
   * Behavior:
   * - Named flags (-i, --input) are extracted first
   * - Positional args are mapped by index, but SKIPPED if their target was
   * already set
   * - Remainder args collect all unmatched positionals
   * - If no unfilled variables remain, extra args are ignored
   */
  /**
   * Parse arguments according to the given definitions.
   * Returns a map of targetPath -> parsed value.
   */
  public static Map<String, Object> parse(String[] args, List<ArgDef> defs) {
    if (defs == null || defs.isEmpty()) {
      return new LinkedHashMap<>();
    }

    DefMaps defMaps = groupDefinitions(defs);
    Map<String, Object> result = new LinkedHashMap<>();
    Set<String> setByName = new HashSet<>();
    List<String> positionals = parseFlags(args, defMaps, result, setByName);
    mapPositionals(positionals, defMaps, result, setByName);

    return result;
  }

  /** Container for grouped argument definitions. */
  private record DefMaps(
      Map<String, ArgDef> shortMap,
      Map<String, ArgDef> longMap,
      Map<Integer, ArgDef> indexMap,
      ArgDef remainder) {
  }

  /** Group definitions by type (short flags, long flags, indices, remainder). */
  private static DefMaps groupDefinitions(List<ArgDef> defs) {
    Map<String, ArgDef> shortMap = new HashMap<>();
    Map<String, ArgDef> longMap = new HashMap<>();
    Map<Integer, ArgDef> indexMap = new TreeMap<>();
    ArgDef remainderDef = null;

    for (ArgDef def : defs) {
      if (def.isRemainder) {
        remainderDef = def;
      } else if (def.index != null) {
        indexMap.put(def.index, def);
      } else {
        if (def.shortOpt != null && !def.shortOpt.isEmpty()) {
          shortMap.put(def.shortOpt, def);
        }
        if (def.longOpt != null && !def.longOpt.isEmpty()) {
          longMap.put(def.longOpt, def);
        }
      }
    }
    DefMaps maps = new DefMaps(shortMap, longMap, indexMap, remainderDef);
    return maps;
  }

  /** Parse named flags and collect positional arguments. */
  private static List<String> parseFlags(String[] args, DefMaps defs,
      Map<String, Object> result, Set<String> setByName) {
    List<String> positionals = new ArrayList<>();
    boolean endOfOptions = false;

    for (int i = 0; i < args.length; i++) {
      String arg = args[i];

      if ("--".equals(arg)) {
        endOfOptions = true;
        continue;
      }

      if (endOfOptions) {
        positionals.add(arg);
        continue;
      }

      if (arg.startsWith("-")) {
        i = processFlag(arg, args, i, defs, result, setByName, positionals);
      } else {
        positionals.add(arg);
      }
    }
    return positionals;
  }

  /** Process a single flag argument. Returns updated index. */
  private static int processFlag(String arg, String[] args, int i, DefMaps defs,
      Map<String, Object> result, Set<String> setByName, List<String> positionals) {
    ArgDef def = null;
    String value = null;

    if (arg.startsWith("--") && arg.contains("=")) {
      // --opt=value
      int eq = arg.indexOf('=');
      String key = arg.substring(0, eq);
      def = defs.longMap().get(key);
      if (def == null) {
        String overrideKey = arg.substring(2, eq);
        if (isDynamicOverrideKey(overrideKey)) {
          // Forward dynamic overrides to CliOverrides path without positional pollution.
          return i;
        }
        throw new IllegalArgumentException("Unknown option: " + key);
      } else {
        value = arg.substring(eq + 1);
      }
    } else if (arg.startsWith("--")) {
      // --opt (standalone long flag, value possibly in next arg)
      def = defs.longMap().get(arg);
      if (def == null) {
        String overrideKey = arg.substring(2);
        if (isDynamicOverrideKey(overrideKey)) {
          return i;
        }
        throw new IllegalArgumentException("Unknown option: " + arg);
      }
      if (!TYPE_BOOLEAN.equalsIgnoreCase(def.type)) {
        if (i + 1 >= args.length || args[i + 1].startsWith("-")) {
          throw new IllegalArgumentException("Missing value for option: " + arg);
        }
        value = args[++i];
      }
    } else if (arg.startsWith("-")) {
      // -o (short flag)
      def = defs.shortMap().get(arg);
      if (def == null) {
        throw new IllegalArgumentException("Unknown option: " + arg);
      }
      if (!TYPE_BOOLEAN.equalsIgnoreCase(def.type)) {
        if (i + 1 >= args.length || args[i + 1].startsWith("-")) {
          throw new IllegalArgumentException("Missing value for option: " + arg);
        }
        value = args[++i];
      }
    }

    if (def != null) {
      result.put(def.targetPath, parseValue(def.type, value));
      setByName.add(def.targetPath);
    }
    return i;
  }

  private static boolean isDynamicOverrideKey(String key) {
    if (key == null || key.isBlank()) {
      return false;
    }
    if (ALLOWED_BARE_OVERRIDES.contains(key)) {
      return true;
    }
    if (!key.contains(".")) {
      return false;
    }
    String topLevel = key.substring(0, key.indexOf('.'));
    return ALLOWED_OVERRIDE_PREFIXES.contains(topLevel);
  }

  /** Map positional arguments to unfilled slots and remainder. */
  private static void mapPositionals(List<String> positionals, DefMaps defs,
      Map<String, Object> result, Set<String> setByName) {
    List<ArgDef> unfilled = defs.indexMap().values().stream()
        .filter(d -> !setByName.contains(d.targetPath))
        .toList();

    List<String> remainderList = new ArrayList<>();
    int unfilledIdx = 0;

    for (String posArg : positionals) {
      if (unfilledIdx < unfilled.size()) {
        ArgDef idxDef = unfilled.get(unfilledIdx++);
        result.put(idxDef.targetPath, parseValue(idxDef.type, posArg));
      } else {
        remainderList.add(posArg);
      }
    }

    if (defs.remainder() != null && !remainderList.isEmpty()
        && !setByName.contains(defs.remainder().targetPath)) {
      result.put(defs.remainder().targetPath, remainderList);
    }
  }

  private static Object parseValue(String type, String value) {
    if (TYPE_BOOLEAN.equalsIgnoreCase(type)) {
      if (value == null) {
        return true;
      }
      if ("true".equalsIgnoreCase(value)) {
        return true;
      }
      if ("false".equalsIgnoreCase(value)) {
        return false;
      }
      throw new IllegalArgumentException("Invalid boolean value: " + value);
    }
    if ("integer".equalsIgnoreCase(type) || "int".equalsIgnoreCase(type)) {
      return Integer.parseInt(value);
    }
    if ("long".equalsIgnoreCase(type)) {
      return Long.parseLong(value);
    }
    // Default: string
    return value;
  }

  /**
   * Build ArgDef list from YAML config list.
   * Supports:
   * - ["-h", "--help", "boolean", "path"]
   * - [0, "string", "path"]
   * - ["string[]", "path"] (remainder)
   */
  @SuppressWarnings("unchecked")
  public static List<ArgDef> buildArgDefs(Object argsConfig) {
    if (argsConfig == null) {
      return List.of();
    }

    if (!(argsConfig instanceof List)) {
      return List.of();
    }

    List<ArgDef> result = new ArrayList<>();
    List<Object> argsList = (List<Object>) argsConfig;

    for (Object entry : argsList) {
      if (!(entry instanceof List<?> list))
        continue;

      // Flags: ["-s", "--long", "type", "path"]
      if (list.size() == 4) {
        Object p0 = list.get(0);
        Object p1 = list.get(1);
        String s0 = (p0 instanceof String) ? (String) p0 : "";
        String s1 = (p1 instanceof String) ? (String) p1 : "";

        if (s0.startsWith("-") || s1.startsWith("--")) {
          String shortOpt = s0.startsWith("-") ? s0 : "";
          String longOpt = s1.startsWith("--") ? s1 : "";
          String type = Objects.toString(list.get(2), "string");
          String targetPath = Objects.toString(list.get(3), "");
          result.add(new ArgDef(shortOpt, longOpt, null, false, type, targetPath));
        }
      }
      // Positional: [0, "type", "path"]
      else if (list.size() == 3 && list.get(0) instanceof Integer idx) {
        String type = Objects.toString(list.get(1), "string");
        String targetPath = Objects.toString(list.get(2), "");
        result.add(new ArgDef(null, null, idx, false, type, targetPath));
      }
      // Remainder: ["string[]", "path"] (or just size 2 list first element string
      // containing [])
      else if (list.size() >= 2 && list.get(0) instanceof String s && s.endsWith("[]")) {
        String targetPath = Objects.toString(list.get(1), "");
        result.add(new ArgDef(null, null, null, true, "list", targetPath));
      }
    }

    return result;
  }

  /**
   * Generate help text from argument definitions.
   */
  public static String generateHelp(String pipelineName, List<ArgDef> globalArgs, List<ArgDef> pipelineArgs) {
    StringBuilder sb = new StringBuilder();
    sb.append("Usage: java -jar pipeline.jar [pipeline] [options] [args...]\n\n");

    if (pipelineName != null) {
      sb.append("Pipeline: ").append(pipelineName).append("\n\n");
    }

    if (!globalArgs.isEmpty()) {
      sb.append("Global Options:\n");
      appendArgHelp(sb, globalArgs);
      sb.append("\n");
    }

    if (!pipelineArgs.isEmpty()) {
      sb.append("Pipeline Options:\n");
      appendArgHelp(sb, pipelineArgs);
    }

    return sb.toString();
  }

  private static void appendArgHelp(StringBuilder sb, List<ArgDef> args) {
    // Flags
    for (ArgDef def : args) {
      if (def.index == null && !def.isRemainder) {
        sb.append(String.format("  %s, %-20s %s%n",
            def.shortOpt.isEmpty() ? "  " : def.shortOpt,
            def.longOpt,
            def.targetPath));
      }
    }

    // Positionals
    boolean hasPos = false;
    for (ArgDef def : args) {
      if (def.index != null) {
        if (!hasPos) {
          sb.append("  [Positional Arguments]:\n");
          hasPos = true;
        }
        sb.append(String.format("    [%d] -> %s (%s)%n", def.index, def.targetPath, def.type));
      }
    }

    // Remainder
    for (ArgDef def : args) {
      if (def.isRemainder) {
        sb.append(String.format("    [...] -> %s (list)%n", def.targetPath));
      }
    }
  }

}
