package pl.gov.nac.warc.config;

import pl.gov.nac.warc.reactive.ReactiveInterfaces;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class ConfigInstantiator {

    private ConfigInstantiator() {
    }

    public static ReactiveInterfaces.ReactiveProducer<?> instantiateProducer(
            String name, ModuleRegistry modules, Map<String, Object> effectiveConfig) throws Exception {

        ModuleDef def = modules.getProducer(name);
        if (def == null) {
            throw new IllegalArgumentException("Unknown producer module: " + name);
        }

        Class<?> cls = Class.forName(def.className);
        Object obj = cls.getDeclaredConstructor().newInstance();

        if (!(obj instanceof ReactiveInterfaces.ReactiveProducer)) {
            throw new IllegalArgumentException("Producer '" + name + "' does not implement ReactiveProducer");
        }

        ReactiveInterfaces.ReactiveProducer<?> p = (ReactiveInterfaces.ReactiveProducer<?>) obj;
        p.configure(effectiveConfig);
        return p;
    }

    public static List<ReactiveInterfaces.ReactiveProcessor<?, ?>> instantiateProcessors(
            List<String> names,
            List<Map<String, Object>> configs,
            ModuleRegistry modules) throws Exception {

        List<ReactiveInterfaces.ReactiveProcessor<?, ?>> out = new ArrayList<>();

        for (int i = 0; i < names.size(); i++) {
            String name = names.get(i);
            Map<String, Object> cfg = configs.get(i);

            ModuleDef def = modules.getProcessor(name);
            if (def == null) {
                throw new IllegalArgumentException("Unknown processor module: " + name);
            }

            Class<?> cls = Class.forName(def.className);
            Object obj = cls.getDeclaredConstructor().newInstance();

            if (!(obj instanceof ReactiveInterfaces.ReactiveProcessor)) {
                throw new IllegalArgumentException("Processor '" + name + "' does not implement ReactiveProcessor");
            }

            ReactiveInterfaces.ReactiveProcessor<?, ?> p = (ReactiveInterfaces.ReactiveProcessor<?, ?>) obj;

            p.configure(cfg);
            out.add(p);
        }

        return out;
    }

    public static ReactiveInterfaces.ReactiveConsumer<?> instantiateConsumer(
            String name, ModuleRegistry modules, Map<String, Object> effectiveConfig) throws Exception {

        ModuleDef def = modules.getConsumer(name);
        if (def == null) {
            throw new IllegalArgumentException("Unknown consumer module: " + name);
        }

        Class<?> cls = Class.forName(def.className);
        Object obj = cls.getDeclaredConstructor().newInstance();

        if (!(obj instanceof ReactiveInterfaces.ReactiveConsumer)) {
            throw new IllegalArgumentException("Consumer '" + name + "' does not implement ReactiveConsumer");
        }

        ReactiveInterfaces.ReactiveConsumer<?> c = (ReactiveInterfaces.ReactiveConsumer<?>) obj;
        c.configure(effectiveConfig);
        return c;
    }

    public static List<ReactiveInterfaces.ReactiveModule> instantiateCheckerList(
            List<String> names,
            List<Map<String, Object>> configs,
            ModuleRegistry modules) throws Exception {

        List<ReactiveInterfaces.ReactiveModule> outModules = new ArrayList<>();

        if (names == null)
            return outModules;

        for (int i = 0; i < names.size(); i++) {
            String name = names.get(i);
            Map<String, Object> cfg = configs.get(i);

            ModuleDef def = modules.getChecker(name);
            if (def == null) {
                throw new IllegalArgumentException("Unknown checker: " + name);
            }

            Class<?> cls = Class.forName(def.className);
            Object obj = cls.getDeclaredConstructor().newInstance();

            if (!(obj instanceof ReactiveInterfaces.ReactiveModule)) {
                throw new IllegalArgumentException("Checker '" + name + "' does not implement ReactiveModule");
            }

            ReactiveInterfaces.ReactiveModule c = (ReactiveInterfaces.ReactiveModule) obj;
            c.configure(cfg);
            outModules.add(c);
        }

        return outModules;
    }
}
