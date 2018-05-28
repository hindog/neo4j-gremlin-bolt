package ta.nemahuta.neo4j.cache;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.ehcache.Cache;
import org.ehcache.config.CacheRuntimeConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DefaultHierarchicalCacheTest {

    @Mock
    private Cache<String, String> parentCacheMock, childCacheMock;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private Cache.Entry<String, String> cacheEntryMock1, cacheEntryMock2;

    @Mock
    private CacheRuntimeConfiguration<String, String> runtimeConfigurationMock;

    private HierarchicalCache<String, String> sut;

    @BeforeEach
    void setupSut() {
        this.sut = new DefaultHierarchicalCache<>(parentCacheMock, childCacheMock);
    }

    @Test
    void commit() {
        // setup: 'stub the invocation necessary for committing the elements'
        when(childCacheMock.iterator()).then(i -> Stream.of(cacheEntryMock1).iterator());
        // when: 'committing the cache'
        sut.commit();
        // then: 'the element of the child cache has been put to the parent cache'
        verify(parentCacheMock, times(1)).put(cacheEntryMock1.getKey(), cacheEntryMock1.getValue());
        verify(childCacheMock, times(1)).remove(cacheEntryMock1.getKey());
        verifyNoMoreInteractions(parentCacheMock, childCacheMock);
    }

    @Test
    void getChildItem() {
        // setup: 'stubbing of the get on the child cache'
        when(childCacheMock.get("x")).thenReturn("y");
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
        verify(childCacheMock, times(1)).put("x", "y");
        verifyNoMoreInteractions(parentCacheMock);
    }

    @Test
    void containsKeyChild() {
        // setup: 'the result from the child mock'
        when(childCacheMock.containsKey("x")).thenReturn(true);
        // when: 'checking if an entry exists'
        assertTrue(sut.containsKey("x"));
        // then: 'the element check is only issued on the child cache'
        verify(childCacheMock, times(1)).containsKey("x");
        verifyNoMoreInteractions(parentCacheMock);
    }

    @Test
    void containsKeyParent() {
        // setup: 'the result from the parent mock'
        when(childCacheMock.containsKey("x")).thenReturn(false);
        when(parentCacheMock.containsKey("x")).thenReturn(true);
        // when: 'checking if an entry exists'
        assertTrue(sut.containsKey("x"));
        // then: 'the element check is issued on both caches'
        verify(childCacheMock, times(1)).containsKey("x");
        verify(parentCacheMock, times(1)).containsKey("x");
    }

    @Test
    void remove() {
        // when: 'removing a key on the cache'
        sut.remove("x");
        sut.remove("x", "y");
        // then: 'the remove is only issued on the child cache'
        verify(childCacheMock, times(1)).remove("x");
        verify(childCacheMock, times(1)).remove("x", "y");
        verifyNoMoreInteractions(parentCacheMock);
    }

    @Test
    void getAll() {
        // setup: 'the responses from child and parent'
        when(parentCacheMock.getAll(ImmutableSet.of("a", "b"))).thenReturn(ImmutableMap.of("x", "y"));
        when(childCacheMock.getAll(ImmutableSet.of("a", "b"))).thenReturn(ImmutableMap.of("a", "b"));
        // when: 'requesting all entries'
        assertEquals(ImmutableMap.of("x", "y", "a", "b"), sut.getAll(ImmutableSet.of("a", "b")));
        // then: 'only one invocation per cache was made'
        verify(parentCacheMock, times(1)).getAll(ImmutableSet.of("a", "b"));
        verify(childCacheMock, times(1)).getAll(ImmutableSet.of("a", "b"));
    }

    @Test
    void putAll() {
        // when: 'putting elements to the cache'
        sut.putAll(ImmutableMap.of("x", "y"));
        // then: 'the put has been invoked on the child cache only'
        verify(childCacheMock, times(1)).putAll(ImmutableMap.of("x", "y"));
        verifyNoMoreInteractions(parentCacheMock);
    }

    @Test
    void removeAll() {
        // when: 'putting elements to the cache'
        sut.removeAll(ImmutableSet.of("x", "y"));
        // then: 'the removal has been invoked on the child cache only'
        verify(childCacheMock, times(1)).removeAll(ImmutableSet.of("x", "y"));
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
    void putIfPresentInParent() {
        // setup: 'stub a result in the parent'
        when(parentCacheMock.get("x")).thenReturn("y");
        // when: 'issuing the operation'
        sut.putIfAbsent("x", "y");
        // then: 'after the get no interactions have been made'
        verify(parentCacheMock, times(1)).get("x");
        verifyNoMoreInteractions(parentCacheMock, childCacheMock);
    }

    @Test
    void putIfAbsentInParent() {
        // when: 'issuing a put if absent'
        sut.putIfAbsent("x", "y");
        // then: 'the operation has been invoked on the child cache only, but the parent cache was queried'
        verify(childCacheMock, times(1)).putIfAbsent("x", "y");
        verify(parentCacheMock, times(1)).get("x");
        verifyNoMoreInteractions(parentCacheMock);
    }

    @Test
    void replace() {
        // when: 'issuing a put if absent'
        sut.replace("x", "y");
        sut.replace("x", "y", "z");
        // then: 'the operation has been invoked on the child cache only'
        verify(childCacheMock, times(1)).replace("x", "y");
        verify(childCacheMock, times(1)).replace("x", "y", "z");
        verifyNoMoreInteractions(parentCacheMock);
    }

    @Test
    void getRuntimeConfiguration() {
        // setup: 'stub the child answer'
        when(childCacheMock.getRuntimeConfiguration()).thenReturn(runtimeConfigurationMock);
        // when: 'requesting the runtime configuration'
        assertEquals(runtimeConfigurationMock, sut.getRuntimeConfiguration());
        // then: 'the operation has been invoked on the child cache only'
        verify(childCacheMock, times(1)).getRuntimeConfiguration();
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
    void iterator() {
        // setup: 'stubbing the iterator functions for both'
        when(cacheEntryMock1.getKey()).thenReturn("x");
        when(cacheEntryMock2.getKey()).thenReturn("x");
        when(childCacheMock.iterator()).then(i -> Stream.of(cacheEntryMock1).iterator());
        when(parentCacheMock.iterator()).then(i -> Stream.of(cacheEntryMock2).iterator());
        // when: 'requesting the iterator'
        final Iterator<Cache.Entry<String, String>> iter = sut.iterator();
        // then: 'the result is unique by their keys'
        assertEquals(ImmutableList.of(cacheEntryMock1), ImmutableList.copyOf(iter));
        // and: 'both caches are queried'
        verify(childCacheMock, times(1)).iterator();
        verify(parentCacheMock, times(1)).iterator();
    }
}