package ta.nemahuta.neo4j.cache;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import javax.cache.Cache;
import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class HierarchicalJCacheTest {

    @Mock
    private Cache<String, String> parentCacheMock;
    @Mock
    private Map<String, Reference<String>> childCacheMock;

    @Mock
    private Cache.Entry<String, String> cacheEntryMock1;

    @Mock
    private Map.Entry<String, Reference<String>> cacheEntryMock2;

    private HierarchicalCache<String, String> sut;

    @BeforeEach
    void setupSut() {
        this.sut = new HierarchicalJCache<>(parentCacheMock, childCacheMock);
        when(parentCacheMock.iterator()).then(i -> ImmutableSet.of(cacheEntryMock1).iterator());
        when(childCacheMock.keySet()).then(i -> ImmutableSet.of("y"));
        when(cacheEntryMock1.getKey()).thenReturn("a");
        when(cacheEntryMock1.getValue()).thenReturn("b");
        when(childCacheMock.entrySet()).thenReturn(ImmutableSet.of(cacheEntryMock2));
        when(cacheEntryMock2.getKey()).thenReturn("x");
        when(cacheEntryMock2.getValue()).thenReturn(new SoftReference<>("y"));
    }

    @Test
    void commit() {
        // when: 'committing the cache'
        sut.commit();
        // then: 'the element of the child cache has been put to the parent cache'
        verify(parentCacheMock, times(1)).put(eq("x"), eq("y"));
        verify(childCacheMock, times(1)).remove(eq("x"));
        verify(childCacheMock, times(1)).entrySet();
        verifyNoMoreInteractions(parentCacheMock, childCacheMock);
    }

    @Test
    void getChildItem() {
        // setup: 'stubbing of the get on the child cache'
        when(childCacheMock.get("x")).thenReturn(new SoftReference<>("y"));
        // when: 'querying the cache'
        assertEquals("y", sut.get("x"));
        // then: 'no interaction was made with the parent'
        verifyNoMoreInteractions(parentCacheMock);
    }

    @Test
    void getMissChildItem() {
        // setup: 'stubbing of the get on the child cache'
        when(parentCacheMock.get("x")).thenReturn("y");
        // when: 'querying the cache'
        assertEquals("y", sut.get("x"));
        // then: 'no interaction was made with the parent'
        verify(childCacheMock).get("x");
    }

    @Test
    void put() {
        // when: 'putting an element to the cache'
        sut.put("x", "y");
        // then: 'the element is put to the child cache only'
        verify(childCacheMock, times(1)).put(eq("x"), argThat(i -> i.get().equals("y")));
        verifyNoMoreInteractions(parentCacheMock);
    }

    @Test
    void remove() {
        // when: 'removing a key on the cache'
        sut.remove("x");
        // then: 'the remove is only issued on the child cache'
        verify(childCacheMock, times(1)).remove("x");
        verifyNoMoreInteractions(parentCacheMock);
    }


    @Test
    void clear() {
        // when: 'putting elements to the cache'
        sut.clear();
        // then: 'the clear has been invoked on the child cache only'
        verify(childCacheMock, times(1)).clear();
        verifyNoMoreInteractions(parentCacheMock);
    }

    @Test
    void removeFromParent() {
        // when: 'issuing the removal'
        final Set<String> keys = ImmutableSet.of("1", "2", "3");
        sut.removeFromParent(keys);
        // then: 'the operation has been invoked on the parent'
        verify(parentCacheMock, times(1)).removeAll(keys);
    }

    @Test
    void getKeys() {
        // when: 'requesting the iterator'
        final Stream<String> keys = sut.getKeys();
        // then: 'the result is unique by their keys'
        assertEquals(ImmutableSet.copyOf(keys.iterator()), ImmutableSet.of("a", "y"));
        // and: 'both caches are queried'
        verify(childCacheMock, times(1)).keySet();
        verify(parentCacheMock, times(1)).iterator();
    }
}