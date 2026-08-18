package pl.gov.nac.warc.config;

import java.util.List;
import java.util.Map;

public final class ModuleDef {
  public final String name;
  public final String className;
  public final String logLevel;
  public final Map<String, Object> config;
  public final List<Object> args;

  public ModuleDef(String name, String className, String logLevel, Map<String, Object> config, List<Object> args) {
    this.name = name;
    this.className = className;
    this.logLevel = logLevel;
    this.config = config == null ? Map.of() : config;
    this.args = args == null ? List.of() : args;
  }
}
