package ta.nemahuta.neo4j.handler;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import ta.nemahuta.neo4j.cache.HierarchicalCache;
import ta.nemahuta.neo4j.query.edge.EdgeQueryBuilder;
import ta.nemahuta.neo4j.query.vertex.VertexQueryBuilder;
import ta.nemahuta.neo4j.scope.Neo4JElementStateScope;
import ta.nemahuta.neo4j.state.Neo4JEdgeState;
import ta.nemahuta.neo4j.state.Neo4JVertexState;
import ta.nemahuta.neo4j.state.VertexEdgeReferences;

import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class DefaultRelationHandlerTest {

    @Mock
    private Neo4JElementStateScope<Neo4JVertexState, VertexQueryBuilder> vertexScope;
    @Mock
    private HierarchicalCache<Long, Neo4JVertexState> vertexCache;
    @Mock
    private Neo4JElementStateScope<Neo4JEdgeState, EdgeQueryBuilder> edgeScope;

    private RelationHandler sut;

    @BeforeEach
    void setupSut() {
        sut = new DefaultRelationHandler(vertexScope, vertexCache, edgeScope);
    }

    @Test
    void getRelationIdsOf() {

    }

    @Nested
    @DisplayName("getRelationIdsOf")
    @ExtendWith(MockitoExtension.class)
    class GetRelatedIds {

        @Mock
        private Neo4JVertexState vertexState;
        @Mock
        private VertexEdgeReferences inRefs, outRefs;
        @Mock
        private Neo4JEdgeState inEdgeState, outEdgeState;
        @Mock(answer = Answers.RETURNS_DEEP_STUBS)
        private EdgeQueryBuilder eqb;


        @BeforeEach
        void stubState() {
            when(vertexState.getIncomingEdgeIds()).thenReturn(inRefs);
            when(vertexState.getOutgoingEdgeIds()).thenReturn(outRefs);
            when(vertexState.getEdgeIds(any())).then(i -> i.getArgument(0) == Direction.IN ? inRefs : outRefs);
            when(inEdgeState.getLabel()).thenReturn("a");
            when(outEdgeState.getLabel()).thenReturn("b");
        }

        @Test
        void retrieveCachedAllLabels() {
            // setup
            when(vertexScope.get(1l)).thenReturn(vertexState);
            when(inRefs.getLabels()).thenReturn(ImmutableSet.of("x", "a"));
            when(outRefs.getLabels()).thenReturn(ImmutableSet.of("y", "b"));
            when(inRefs.get("x")).thenReturn(ImmutableSet.of(1l));
            when(outRefs.get("y")).thenReturn(ImmutableSet.of(2l));
            when(edgeScope.queryAndCache(any())).then(i -> {
                i.<Function>getArgument(0).apply(eqb);
                return ImmutableMap.of(3l, inEdgeState, 4l, outEdgeState);
            });
            assertEquals(ImmutableSet.of(1l, 2l, 3l, 4l), ImmutableSet.copyOf(sut.getRelationIdsOf(1l, Direction.BOTH, ImmutableSet.of()).iterator()));
        }

        @Test
        void retrieveLabelsNoState() {
            assertEquals(ImmutableSet.of(), ImmutableSet.copyOf(sut.getRelationIdsOf(1l, Direction.BOTH, ImmutableSet.of()).iterator()));
        }
    }
}