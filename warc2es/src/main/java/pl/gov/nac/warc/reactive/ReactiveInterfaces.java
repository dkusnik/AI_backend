package pl.gov.nac.warc.reactive;

import pl.gov.nac.warc.records.Record;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Flow;

/**
 * Unified reactive module interfaces with before/after checkers.
 */
public final class ReactiveInterfaces {

    private ReactiveInterfaces() {
    }

    // ============================================================
    // COMMON BASE INTERFACE
    // ============================================================

    public interface ReactiveModule {
        void configure(Map<String, Object> cfg);

        /**
         * BEFORE CHECKER
         * Called before pipeline execution.
         * If returns false → pipeline is skipped.
         */
        default boolean beforeCheck(Map<String, Object> cfg) {
            return true;
        }

        /**
         * AFTER CHECKER
         * Called after pipeline execution.
         * Return exit code (0 = success).
         */
        default int afterCheck(Map<String, Object> cfg) {
            return 0;
        }

        /**
         * Whether this module is functionally enabled with its current configuration.
         * Disabled modules are skipped during pipeline execution and type negotiation.
         * 
         * @param cfg effective module configuration
         * @return true if enabled (default)
         */
        default boolean isEnabled(Map<String, Object> cfg) {
            return true;
        }

        // ============================================================
        // TYPE COMPATIBILITY DECLARATIONS
        // ============================================================

        /**
         * Record types this module can accept as input (ordered by preference).
         * Used for format negotiation between module pairs.
         * Can be overridden via configuration (e.g. consumer.acceptedInputTypes).
         * 
         * @return ordered list of accepted record classes, or empty if unspecified
         */
        default List<Class<? extends Record>> acceptedInputTypes() {
            return List.of();
        }

        /**
         * Record types this module may emit as output (ordered by preference).
         * Used for format negotiation between module pairs.
         * Can be overridden via configuration (e.g. producer.emittedOutputTypes).
         * 
         * @return ordered list of emitted record classes, or empty if unspecified
         */
        default List<Class<? extends Record>> emittedOutputTypes() {
            return List.of();
        }

        /**
         * Ordered list of preferred output formats (first = most preferred).
         * Used for format negotiation when multiple types are supported.
         * 
         * @return ordered list of preferred output classes
         */
        default List<Class<? extends Record>> preferredOutputFormats() {
            return List.of();
        }

        /**
         * Whether this module changes the class of the record it processes.
         * If false, the engine will prefer to maintain the same record class for
         * performance,
         * bypassing translation if the next module accepts it.
         * 
         * @return true if record class may change (default)
         */
        default boolean doesChangeRecordClass() {
            return true;
        }

        /**
         * Called by the engine after negotiation to inform the module of its input
         * type.
         */
        default void onNegotiatedInputType(Class<?> type) {
            // Optional hook
        }

        /**
         * Called by the engine after negotiation to inform the module of its output
         * type.
         */
        default void onNegotiatedOutputType(Class<?> type) {
            // Optional hook
        }
    }

    // ============================================================
    // PRODUCER
    // ============================================================

    public interface ReactiveProducer<T>
            extends ReactiveModule, Flow.Publisher<T> {

        /**
         * Start producing items into the reactive pipeline.
         * Called by the engine after configuration and checks.
         */
        void startProducing();
    }

    // ============================================================
    // PROCESSOR
    // ============================================================

    public interface ReactiveProcessor<I, O>
            extends ReactiveModule, Flow.Processor<I, O> {
    }

    // ============================================================
    // CONSUMER
    // ============================================================

    public interface ReactiveConsumer<T>
            extends ReactiveModule, Flow.Subscriber<T> {
        /**
         * Optional lifecycle hook to initialize consuming (e.g. open files).
         */
        default void startConsuming() {
        }

        /**
         * Publish outputs after processing and every after-check have succeeded.
         * Consumers that do not own filesystem outputs keep the no-op default.
         *
         * @return zero on success, otherwise a process exit code
         */
        default int publishOutputs() {
            return 0;
        }

        /**
         * Discard unpublished output state after any failure before publication.
         * Implementations must not remove outputs that were already published.
         */
        default void discardOutputs() {
        }
    }
}
