package io.flinkstate.inspector.reader;

import org.apache.flink.shaded.asm9.org.objectweb.asm.ClassWriter;
import org.apache.flink.shaded.asm9.org.objectweb.asm.Opcodes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * ClassLoader that generates stub classes for types not found on the classpath.
 * This allows reading Flink checkpoint metadata without needing the application's
 * domain classes (e.g., protobuf types, custom POJOs) that were used in the Flink job.
 */
final class LenientClassLoader extends ClassLoader {

    private static final Logger LOG = LoggerFactory.getLogger(LenientClassLoader.class);
    private final ConcurrentHashMap<String, Class<?>> generatedClasses = new ConcurrentHashMap<>();

    LenientClassLoader(ClassLoader parent) {
        super(parent);
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
        try {
            return super.loadClass(name);
        } catch (ClassNotFoundException e) {
            return generatedClasses.computeIfAbsent(name, this::generateStubClass);
        }
    }

    private Class<?> generateStubClass(String name) {
        LOG.debug("Generating stub class for missing type: {}", name);
        String internalName = name.replace('.', '/');
        ClassWriter cw = new ClassWriter(0);
        cw.visit(Opcodes.V17, Opcodes.ACC_PUBLIC, internalName, null,
            "java/lang/Object", new String[]{"java/io/Serializable"});
        cw.visitEnd();
        byte[] bytecode = cw.toByteArray();
        return defineClass(name, bytecode, 0, bytecode.length);
    }
}
