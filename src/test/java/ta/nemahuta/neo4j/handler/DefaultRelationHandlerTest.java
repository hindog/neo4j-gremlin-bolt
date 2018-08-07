package ta.nemahuta.neo4j.handler;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ta.nemahuta.neo4j.cache.HierarchicalCache;
import ta.nemahuta.neo4j.query.edge.EdgeQueryBuilder;
import ta.nemahuta.neo4j.query.vertex.VertexQueryBuilder;
import ta.nemahuta.neo4j.scope.Neo4JElementStateScope;
import ta.nemahuta.neo4j.state.Neo4JEdgeState;
import ta.nemahuta.neo4j.state.Neo4JVertexState;

@ExtendWith(MockitoExtension.class)
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

    @Nested
    @DisplayName("GetRelatedIds")
    class GetRelatedIds {

        @Test
        void issueTest() {
            // TODO implement this
        }

    }
}