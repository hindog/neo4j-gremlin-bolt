package ta.nemahuta.neo4j.structure;

import org.junit.jupiter.api.Test;

import java.lang.ref.SoftReference;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class SoftRefMapTest {

    private final Map<String, SoftReference<String>> internalMap = new HashMap<>();
    private final SoftRefMap<String, String> sut = new SoftRefMap<>(internalMap);

    @Test
    void getOrCreateForNonExistingKey() {
        // when: 'requesting an entry which is not present'
        assertEquals("B", sut.getOrCreate("A", () -> "B"));
        // then: 'a soft reference ist stored for it'
        assertNotNull(internalMap.get("A"));
    }

    @Test
    void getOrCreateForNullSoftReference() {
        // setup: 'an entry which is removed'
        internalMap.put("A", new SoftReference<>(null));
        // when: 'requesting an entry which is present but null'
        assertEquals("B", sut.getOrCreate("A", () -> "B"));
        // then: 'a soft reference ist stored for it'
        assertNotNull(internalMap.get("A"));
    }

    @Test
    void construct() {
        assertNotNull(new SoftRefMap<String, String>());
    }

}