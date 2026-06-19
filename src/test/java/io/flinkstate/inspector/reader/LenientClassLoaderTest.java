package io.flinkstate.inspector.reader;

import org.junit.jupiter.api.Test;

import java.io.Serializable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class LenientClassLoaderTest {

    @Test
    void loadExistingClassReturnsRealClass() throws Exception {
        // Arrange
        LenientClassLoader cl = new LenientClassLoader(getClass().getClassLoader());

        // Act
        Class<?> result = cl.loadClass("java.lang.String");

        // Assert
        assertThat(result).isEqualTo(String.class);
    }

    @Test
    void loadMissingClassGeneratesStub() throws Exception {
        // Arrange
        LenientClassLoader cl = new LenientClassLoader(getClass().getClassLoader());

        // Act
        Class<?> result = cl.loadClass("com.example.nonexistent.FakeClass");

        // Assert
        assertThat(result).isNotNull();
        assertThat(result.getName()).isEqualTo("com.example.nonexistent.FakeClass");
        assertThat(Serializable.class).isAssignableFrom(result);
    }

    @Test
    void multipleDistinctClassesAreIndependent() throws Exception {
        // Arrange
        LenientClassLoader cl = new LenientClassLoader(getClass().getClassLoader());

        // Act
        Class<?> classA = cl.loadClass("com.test.StubClassA");
        Class<?> classB = cl.loadClass("com.test.StubClassB");

        // Assert
        assertThat(classA).isNotEqualTo(classB);
        assertThat(classA.getName()).isNotEqualTo(classB.getName());
    }

    @Test
    void sameClassLoadedTwiceReturnsSameInstance() throws Exception {
        // Arrange
        LenientClassLoader cl = new LenientClassLoader(getClass().getClassLoader());
        String className = "com.test.CachedStubClass";

        // Act
        Class<?> first = cl.loadClass(className);
        Class<?> second = cl.loadClass(className);

        // Assert
        assertThat(first).isSameAs(second);
    }

    @Test
    void stubClassExtendsObject() throws Exception {
        // Arrange
        LenientClassLoader cl = new LenientClassLoader(getClass().getClassLoader());

        // Act
        Class<?> stub = cl.loadClass("com.test.SuperclassCheck");

        // Assert
        assertThat(stub.getSuperclass()).isEqualTo(Object.class);
    }
}
