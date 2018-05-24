package ta.nemahuta.neo4j;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.driver.v1.AuthTokens;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xmlunit.matchers.CompareMatcher;
import ta.nemahuta.neo4j.config.Neo4JConfiguration;
import ta.nemahuta.neo4j.structure.Neo4JGraphFactory;

import javax.annotation.Nonnull;
import java.io.*;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;

class SimpleCreateAndLoadTest {

    private static final UUID uuid = UUID.randomUUID();
    private static final Logger log = LoggerFactory.getLogger(SimpleCreateAndLoadTest.class);

    private Neo4JGraphFactory graphFactory = createFactory();

    SimpleCreateAndLoadTest() throws Exception {
    }

    @Nonnull
    private Neo4JGraphFactory createFactory() throws Exception {
        closeFactory();
        return graphFactory = new Neo4JGraphFactory(
                Neo4JConfiguration.builder()
                        .graphName(uuid.toString())
                        .hostname("localhost")
                        .port(7687)
                        .authToken(AuthTokens.basic("neo4j", "neo4j123")).build()
        );
    }

    @AfterEach
    void closeFactory() throws Exception {
        if (graphFactory != null) {
            graphFactory.close();
            graphFactory = null;
        }
    }


    @ParameterizedTest
    @ValueSource(strings = {"/graph1-example.xml", "/graph2-example.xml"})
    void createGraphCloseAndLoad(final String source) throws Exception {
        streamGraph(source);
        compareGraph(source);
        clearGraph();
    }

    void clearGraph() throws Exception {
        if (graphFactory != null) {
            try (Graph g = graphFactory.get()) {
                try (Transaction tx = g.tx()) {
                    g.vertices().forEachRemaining(Vertex::remove);
                    tx.commit();
                }
            }
        }
    }

    private void compareGraph(final String source) throws Exception {
        try (OutputStream os = new ByteArrayOutputStream()) {
            try (Graph graph = graphFactory.get()) {
                try (Transaction tx = graph.tx()) {
                    graph.io(IoCore.graphml()).writer().create().writeGraph(os, graph);
                    tx.rollback();
                }
            }
            log.debug("\n", new String(((ByteArrayOutputStream) os).toByteArray()));
            try (Reader expected = new InputStreamReader(getClass().getResourceAsStream(source))) {
                try (Reader actual = new InputStreamReader(new ByteArrayInputStream(((ByteArrayOutputStream) os).toByteArray()))) {
                    assertThat(expected, CompareMatcher.isIdenticalTo(actual));
                }
            }
        }
    }

    private void streamGraph(@Nonnull final String source) throws Exception {
        try (Graph graph = graphFactory.get()) {
            try (Transaction tx = graph.tx()) {
                try (InputStream is = getClass().getResourceAsStream(source)) {
                    graph.io(IoCore.graphml()).reader().create().readGraph(is, graph);
                }
                tx.commit();
            }
        }
    }

}
