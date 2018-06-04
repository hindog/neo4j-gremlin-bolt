package ta.nemahuta.neo4j.cache;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.cache.Cache;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class HierarchicalJCacheTest {

    @Mock
    private Cache<String, String> parentCacheMock, childCacheMock;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private Cache.Entry<String, String> cacheEntryMock1, cacheEntryMock2;

    private HierarchicalCache<String, String> sut;

    @BeforeEach
    void setupSut() {
        this.sut = new HierarchicalJCache<>(parentCacheMock, childCacheMock);
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