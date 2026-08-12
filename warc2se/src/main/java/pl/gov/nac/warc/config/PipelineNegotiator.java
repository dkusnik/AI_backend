package pl.gov.nac.warc.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pl.gov.nac.warc.reactive.ReactiveInterfaces.ReactiveModule;
import pl.gov.nac.warc.records.Record;

/**
 * Negotiates record types between pipeline modules.
 *
 * <p>
 * The negotiator:
 * <ul>
 * <li>Filters disabled modules from the chain</li>
 * <li>Validates type compatibility between adjacent modules</li>
 * <li>Selects optimal record types based on preferences</li>
 * <li>Notifies modules of their negotiated input/output types</li>
 * </ul>
 */
public final class PipelineNegotiator {

  private static final Logger log = LogManager.getLogger(PipelineNegotiator.class);

  private PipelineNegotiator() {
    // Static utility class
  }

  /**
   * Result of pipeline negotiation.
   */
  public record NegotiationResult(
      List<ReactiveModule> activeModules,
      List<Class<? extends Record>> negotiatedTypes,
      List<String> messages) {
    public boolean isSuccess() {
      return activeModules != null && !activeModules.isEmpty();
    }
  }

  /**
   * Negotiates the pipeline chain, filtering disabled modules and validating
   * types.
   *
   * @param modules ordered list of modules in the pipeline
   * @param configs corresponding configuration for each module
   * @return negotiation result with active modules and types
   */
  public static NegotiationResult negotiate(
      List<? extends ReactiveModule> modules,
      List<Map<String, Object>> configs) {

    List<ReactiveModule> active = new ArrayList<>();
    List<Class<? extends Record>> types = new ArrayList<>();
    List<String> messages = new ArrayList<>();

    // Step 1: Filter disabled modules
    for (int i = 0; i < modules.size(); i++) {
      ReactiveModule module = modules.get(i);
      Map<String, Object> cfg = i < configs.size() ? configs.get(i) : Map.of();

      if (module.isEnabled(cfg)) {
        active.add(module);
      } else {
        messages.add("Skipped: " + module.getClass().getSimpleName() + " (disabled)");
      }
    }

    if (active.isEmpty()) {
      messages.add("ERROR: No enabled modules in pipeline");
      PipelineContext.setExitCode(PipelineContext.EXIT_NEGOTIATION_FAILED);
      return new NegotiationResult(List.of(), List.of(), messages);
    }

    // Step 2: Negotiate types between adjacent modules
    Class<? extends Record> currentType = null;

    for (int i = 0; i < active.size(); i++) {
      ReactiveModule module = active.get(i);

      // First module: select output type compatible with next module
      if (i == 0) {
        List<Class<? extends Record>> outputs = module.emittedOutputTypes();
        if (!outputs.isEmpty()) {
          // Look ahead at what next module accepts if available
          if (active.size() > 1) {
            List<Class<? extends Record>> nextAccepts = active.get(1).acceptedInputTypes();
            // Find best match: prefer types that appear earlier in both lists
            currentType = findBestMatch(outputs, nextAccepts);
            log.debug("TYPE NEGOTIATION: producer={} outputs={}, next accepts={}, selected={}",
                module.getClass().getSimpleName(),
                outputs.stream().map(Class::getSimpleName).collect(Collectors.joining(",")),
                nextAccepts.stream().map(Class::getSimpleName).collect(Collectors.joining(",")),
                currentType != null ? currentType.getSimpleName() : "null");
            if (currentType == null) {
              currentType = outputs.get(0); // Fallback to first
            }
          } else {
            currentType = outputs.get(0);
          }
          types.add(currentType);
          module.onNegotiatedOutputType(currentType);
        }
        continue;
      }

      // Subsequent modules: check compatibility
      List<Class<? extends Record>> accepted = module.acceptedInputTypes();

      if (!accepted.isEmpty() && currentType != null) {
        // Find compatible type
        Class<? extends Record> compatible = findCompatibleType(currentType, accepted);

        if (compatible == null) {
          String error = String.format(
              "ERROR: %s cannot accept %s (needs: %s)",
              module.getClass().getSimpleName(),
              currentType.getSimpleName(),
              accepted.stream().map(Class::getSimpleName).collect(Collectors.joining(", ")));
          messages.add(error);
          PipelineContext.setExitCode(PipelineContext.EXIT_NEGOTIATION_FAILED);
          return new NegotiationResult(List.of(), List.of(), messages);
        }

        module.onNegotiatedInputType(compatible);
        types.add(compatible);
      }

      // Update output type
      List<Class<? extends Record>> outputs = module.emittedOutputTypes();
      if (!outputs.isEmpty()) {
        currentType = outputs.get(0);
        module.onNegotiatedOutputType(currentType);
      } else if (!module.doesChangeRecordClass() && currentType != null) {
        // Pass-through: maintain current type
        module.onNegotiatedOutputType(currentType);
      }
    }

    messages.add("Negotiation successful: " + active.size() + " modules active");
    return new NegotiationResult(active, types, messages);
  }

  /**
   * Finds the best matching type from producer outputs that consumer accepts.
   * Prefers types that appear earlier in the accepted list (higher priority).
   */
  private static Class<? extends Record> findBestMatch(
      List<Class<? extends Record>> producerOutputs,
      List<Class<? extends Record>> consumerAccepts) {

    // Try each accepted type in order (priority order)
    for (Class<? extends Record> accepted : consumerAccepts) {
      if (producerOutputs.contains(accepted)) {
        return accepted;
      }
    }

    // No direct match, try assignability
    for (Class<? extends Record> accepted : consumerAccepts) {
      for (Class<? extends Record> output : producerOutputs) {
        if (accepted.isAssignableFrom(output)) {
          return output;
        }
      }
    }

    return null;
  }

  /**
   * Finds a compatible type from accepted types that matches the current type.
   */
  private static Class<? extends Record> findCompatibleType(
      Class<? extends Record> current,
      List<Class<? extends Record>> accepted) {

    // Direct match
    if (accepted.contains(current)) {
      return current;
    }

    // Check for superclass/interface match
    for (Class<? extends Record> acceptedType : accepted) {
      if (acceptedType.isAssignableFrom(current)) {
        return current;
      }
    }

    return null;
  }

  /**
   * Formats the negotiation result for display.
   */
  public static String formatResult(
      List<? extends ReactiveModule> original,
      NegotiationResult result) {

    StringBuilder sb = new StringBuilder();
    sb.append("Original: [");
    sb.append(original.stream()
        .map(m -> m.getClass().getSimpleName())
        .collect(Collectors.joining(", ")));
    sb.append("]\n");

    sb.append("Active: [");
    sb.append(result.activeModules().stream()
        .map(m -> m.getClass().getSimpleName())
        .collect(Collectors.joining(", ")));
    sb.append("]\n");

    for (String msg : result.messages()) {
      sb.append("  ").append(msg).append("\n");
    }

    return sb.toString();
  }
}
