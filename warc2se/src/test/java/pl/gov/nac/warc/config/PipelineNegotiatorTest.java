package pl.gov.nac.warc.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import pl.gov.nac.warc.reactive.ReactiveInterfaces.ReactiveModule;
import pl.gov.nac.warc.records.Record;
import pl.gov.nac.warc.records.RecordExternal;
import pl.gov.nac.warc.records.RecordInMemory;

class PipelineNegotiatorTest {

  // Test Module Stub
  static class TestModule implements ReactiveModule {
    private final String name;
    private final boolean enabled;
    private final List<Class<? extends Record>> emitted;
    private final List<Class<? extends Record>> accepted;
    private final boolean changesClass;

    TestModule(String name, boolean enabled,
        List<Class<? extends Record>> emitted,
        List<Class<? extends Record>> accepted,
        boolean changesClass) {
      this.name = name;
      this.enabled = enabled;
      this.emitted = emitted;
      this.accepted = accepted;
      this.changesClass = changesClass;
    }

    @Override
    public void configure(Map<String, Object> cfg) {
    }

    @Override
    public boolean isEnabled(Map<String, Object> cfg) {
      return enabled;
    }

    @Override
    public List<Class<? extends Record>> emittedOutputTypes() {
      return emitted;
    }

    @Override
    public List<Class<? extends Record>> acceptedInputTypes() {
      return accepted;
    }

    @Override
    public boolean doesChangeRecordClass() {
      return changesClass;
    }

    @Override
    public String toString() {
      return name;
    }
  }

  @Test
  void testCompatibleTypes() {
    // A executes -> B accepts A
    TestModule modA = new TestModule("A", true, List.of(RecordInMemory.class), List.of(), true);
    TestModule modB = new TestModule("B", true, List.of(RecordExternal.class), List.of(RecordInMemory.class), true);

    var result = PipelineNegotiator.negotiate(List.of(modA, modB), List.of(Map.of(), Map.of()));

    assertTrue(result.isSuccess());
    assertEquals(2, result.activeModules().size());
  }

  @Test
  void testIncompatibleTypes() {
    // A emits InMemory -> B accepts External
    TestModule modA = new TestModule("A", true, List.of(RecordInMemory.class), List.of(), true);
    TestModule modB = new TestModule("B", true, List.of(RecordExternal.class), List.of(RecordExternal.class), true);

    var result = PipelineNegotiator.negotiate(List.of(modA, modB), List.of(Map.of(), Map.of()));

    assertFalse(result.isSuccess());
    assertTrue(result.messages().stream().anyMatch(m -> m.contains("cannot accept RecordInMemory")));
  }

  @Test
  void testMultipleAcceptedTypes() {
    // A emits InMemory -> B accepts [External, InMemory]
    TestModule modA = new TestModule("A", true, List.of(RecordInMemory.class), List.of(), true);
    TestModule modB = new TestModule("B", true, List.of(RecordExternal.class),
        List.of(RecordExternal.class, RecordInMemory.class), true);

    var result = PipelineNegotiator.negotiate(List.of(modA, modB), List.of(Map.of(), Map.of()));

    assertTrue(result.isSuccess());
  }

  @Test
  void testDisabledModules() {
    // A emits InMemory -> B (Disabled, accepts External) -> D (Accepts InMemory)
    // Should skip B, and A->D should match
    TestModule modA = new TestModule("A", true, List.of(RecordInMemory.class), List.of(), true);
    TestModule modB = new TestModule("B", false, List.of(RecordExternal.class), List.of(RecordExternal.class), true);
    TestModule modD = new TestModule("D", true, List.of(RecordExternal.class), List.of(RecordInMemory.class), true);

    var result = PipelineNegotiator.negotiate(List.of(modA, modB, modD), List.of(Map.of(), Map.of(), Map.of()));

    assertTrue(result.isSuccess());
    assertEquals(2, result.activeModules().size());
    assertEquals("A", result.activeModules().get(0).toString());
    assertEquals("D", result.activeModules().get(1).toString());
  }

  @Test
  void testPassThrough() {
    // A emits InMemory -> B (Pass-through, accepts InMemory) -> C (Accepts
    // InMemory)
    TestModule modA = new TestModule("A", true, List.of(RecordInMemory.class), List.of(), true);
    // B does NOT change record class, so it emits what it inputs
    TestModule modB = new TestModule("B", true, List.of(), List.of(RecordInMemory.class), false);
    TestModule modC = new TestModule("C", true, List.of(), List.of(RecordInMemory.class), true);

    var result = PipelineNegotiator.negotiate(List.of(modA, modB, modC), List.of(Map.of(), Map.of(), Map.of()));

    assertTrue(result.isSuccess());
    // Negotiator should track output of B as A
    // In current impl, it checks 'emittedOutputTypes'. If empty and
    // !doesChangeRecordClass, it keeps currentType.
  }
}
