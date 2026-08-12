package pl.gov.nac.warc.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import pl.gov.nac.warc.config.ArgParser.ArgDef;

class ArgParserTest {

  private static final List<ArgDef> DEFINITIONS = ArgParser.buildArgDefs(List.of(
      List.of("-v", "--verbose", "boolean", "global.verbose"),
      List.of("-n", "--count", "integer", "global.count"),
      List.of("-o", "--output", "string", "output.file"),
      List.of(0, "string", "output.file"),
      List.of("string[]", "input.files")));

  @Test
  void buildsDefinitionsForFlagsPositionalsAndRemainder() {
    assertEquals(5, DEFINITIONS.size());
    assertEquals(new ArgDef("-v", "--verbose", null, false, "boolean", "global.verbose"),
        DEFINITIONS.get(0));
    assertEquals(new ArgDef(null, null, 0, false, "string", "output.file"),
        DEFINITIONS.get(3));
    assertEquals(new ArgDef(null, null, null, true, "list", "input.files"),
        DEFINITIONS.get(4));
  }

  @Test
  void namedValueWinsAndUnclaimedPositionalsBecomeRemainder() {
    Map<String, Object> parsed = ArgParser.parse(new String[] {
        "--verbose",
        "--count=7",
        "--output", "named.warc",
        "positional.warc",
        "extra.warc"
    }, DEFINITIONS);

    assertEquals(true, parsed.get("global.verbose"));
    assertEquals(7, parsed.get("global.count"));
    assertEquals("named.warc", parsed.get("output.file"));
    assertEquals(List.of("positional.warc", "extra.warc"), parsed.get("input.files"));
  }

  @Test
  void endOfOptionsMakesDashPrefixedValuesPositional() {
    Map<String, Object> parsed = ArgParser.parse(
        new String[] { "--", "-literal-output", "--literal-input" },
        DEFINITIONS);

    assertEquals("-literal-output", parsed.get("output.file"));
    assertEquals(List.of("--literal-input"), parsed.get("input.files"));
  }

  @Test
  void rejectsUnknownOptionsMissingValuesAndInvalidBooleans() {
    assertThrows(IllegalArgumentException.class,
        () -> ArgParser.parse(new String[] { "--unknown" }, DEFINITIONS));
    assertThrows(IllegalArgumentException.class,
        () -> ArgParser.parse(new String[] { "--count" }, DEFINITIONS));
    assertThrows(IllegalArgumentException.class,
        () -> ArgParser.parse(new String[] { "--verbose=maybe" }, DEFINITIONS));
  }

}
