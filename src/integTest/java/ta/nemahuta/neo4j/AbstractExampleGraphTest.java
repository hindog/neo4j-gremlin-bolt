package ta.nemahuta.neo4j;

import com.google.common.collect.ImmutableList;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.neo4j.driver.v1.AuthTokens;
import org.xmlunit.matchers.CompareMatcher;
import ta.nemahuta.neo4j.config.Neo4JConfiguration;
import ta.nemahuta.neo4j.structure.Neo4JGraphFactory;

import javax.annotation.Nonnull;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

public abstract class AbstractExampleGraphTest {

    private static InheritableThreadLocal<Neo4JGraphFactory> TL_GRAPH_FACTORY = new InheritableThreadLocal<>();

    public interface GraphParams {
        String getSource();

        int getVerticesCount();

        int getEdgesCount();
    }

    public enum ExampleGraphs implements GraphParams {

        SIMPLE("/graph1-example.xml", 6, 6),
        COMPLEX("/graph2-example.xml", 809, 8049);

        private final String source;
        private final int verticesCount, edgesCount;

        ExampleGraphs(final String source, final int verticesCount, final int edgesCount) {
            this.source = source;
            this.verticesCount = verticesCount;
            this.edgesCount = edgesCount;
        }

        @Override
        public String getSource() {
            return source;
        }

        @Override
        public int getVerticesCount() {
            return verticesCount;
        }

        @Override
        public int getEdgesCount() {
            return edgesCount;
        }
    }


    protected Neo4JGraphFactory getGraphFactory() {
        return TL_GRAPH_FACTORY.get();
    }

    @BeforeAll
    static private void createFactory() throws URISyntaxException {
        closeFactory();
        TL_GRAPH_FACTORY.set(new Neo4JGraphFactory(
                Neo4JConfiguration.builder()
                        .graphName(UUID.randomUUID().toString())
                        .hostname("localhost")
                        .port(7687)
                        .cacheConfiguration(AbstractExampleGraphTest.class.getResource("/ehCache.xml").toURI())
                        .cacheDisabled(true)
                        .authToken(AuthTokens.basic("neo4j", "neo4j123")).build()
        ));
    }

    @AfterAll
    static void closeFactory() {
        Optional.ofNullable(TL_GRAPH_FACTORY.get()).ifPresent(Neo4JGraphFactory::close);
    }

    @AfterEach
    void cleanupGraph() throws Exception {
        Optional.ofNullable(TL_GRAPH_FACTORY.get()).ifPresent(g -> clearGraph());
    }

    private void clearGraph() {
        try {
            withGraph(graph -> {
                graph.vertices().forEachRemaining(Vertex::remove);
                graph.tx().commit();
            });
        } catch (final Exception ex) {
            throw new IllegalStateException("Could not clear graph.", ex);
        }
    }


    protected void checkGraph(final GraphParams params) throws Exception {
        streamGraph(params.getSource());
        compareGraph(params.getSource(), params.getVerticesCount(), params.getEdgesCount());
    }

    protected <T> T withGraph(final GraphFunction<T> callable) throws Exception {
        try (Graph graph = TL_GRAPH_FACTORY.get().get()) {
            try (Transaction tx = graph.tx()) {
                return callable.apply(graph);
            }
        }
    }

    protected void withGraph(final GraphConsumer consumer) throws Exception {
        withGraph(graph -> {
            consumer.consume(graph);
            return null;
        });
    }

    protected void compareGraph(final String source, final long vertexCount, final long edgeCount) throws Exception {
        try (OutputStream os = new ByteArrayOutputStream()) {
            withGraph(graph -> {
                assertEquals(vertexCount, ImmutableList.copyOf(graph.vertices()).size());
                assertEquals(edgeCount, ImmutableList.copyOf(graph.edges()).size());
                graph.io(IoCore.graphml()).writer().create().writeGraph(os, graph);
                graph.tx().rollback();
            });
            try (Reader expected = new InputStreamReader(getClass().getResourceAsStream(source))) {
                try (Reader actual = new InputStreamReader(new ByteArrayInputStream(((ByteArrayOutputStream) os).toByteArray()))) {
                    assertThat(expected, CompareMatcher.isIdenticalTo(actual));
                }
            }
        }
    }

    protected void streamGraph(@Nonnull final String source) throws Exception {
        withGraph(graph -> {
            try (InputStream is = getClass().getResourceAsStream(source)) {
                graph.io(IoCore.graphml()).reader().create().readGraph(is, graph);
            }
        });
    }

    @FunctionalInterface
    public interface GraphFunction<T> {
        T apply(Graph graph) throws Exception;
    }

    @FunctionalInterface
    public interface GraphConsumer {
        void consume(Graph graph) throws Exception;
    }
}