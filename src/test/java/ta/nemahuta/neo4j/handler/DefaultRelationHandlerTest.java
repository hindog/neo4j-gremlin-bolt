package ta.nemahuta.neo4j.handler;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import ta.nemahuta.neo4j.query.edge.EdgeQueryBuilder;
import ta.nemahuta.neo4j.query.vertex.VertexQueryBuilder;
import ta.nemahuta.neo4j.scope.Neo4JElementStateScope;
import ta.nemahuta.neo4j.state.Neo4JEdgeState;
import ta.nemahuta.neo4j.state.Neo4JVertexState;

class DefaultRelationHandlerTest {

    @Mock
    private Neo4JElementStateScope<Neo4JVertexState, VertexQueryBuilder> vertexScope;
    @Mock
    private Neo4JElementStateScope<Neo4JEdgeState, EdgeQueryBuilder> edgeScope;

    private RelationHandler sut;

    @BeforeEach
    void setupSut() {
        sut = new DefaultRelationHandler(vertexScope, edgeScope);
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