package pl.gov.nac.warc.config;

import java.util.LinkedHashMap;
import java.util.Map;

public final class ModuleRegistry {
    public final Map<String, ModuleDef> producers = new LinkedHashMap<>();
    public final Map<String, ModuleDef> processors = new LinkedHashMap<>();
    public final Map<String, ModuleDef> consumers = new LinkedHashMap<>();
    public final Map<String, ModuleDef> checkers = new LinkedHashMap<>();

    public ModuleDef getProducer(String name) {
        return producers.get(name);
    }

    public ModuleDef getProcessor(String name) {
        return processors.get(name);
    }

    public ModuleDef getConsumer(String name) {
        return consumers.get(name);
    }

    public ModuleDef getChecker(String name) {
        return checkers.get(name);
    }
}
