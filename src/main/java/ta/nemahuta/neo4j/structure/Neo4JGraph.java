package ta.nemahuta.neo4j.structure;

import lombok.Getter;
import lombok.NonNull;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactoryClass;
import ta.nemahuta.neo4j.features.Neo4Features;
import ta.nemahuta.neo4j.config.Neo4JConfiguration;
import ta.nemahuta.neo4j.session.Neo4JSession;
import ta.nemahuta.neo4j.session.Neo4JTransaction;

import javax.annotation.Nonnull;
import java.util.Iterator;

/**
 * The Neo4J implementation for a {@link Graph}.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
@GraphFactoryClass(Neo4JGraphFactory.class)
public class Neo4JGraph implements Graph, AutoCloseable {

    @Getter
    private final Neo4JSession session;
    private final Neo4JTransaction transaction;
    private final Neo4JConfiguration configuration;

    public Neo4JGraph(@Nonnull @NonNull final Neo4JSession session,
                      @Nonnull @NonNull final Neo4JConfiguration configuration) {
        this.session = session;
        this.transaction = new Neo4JTransaction(this, this.session);
        this.configuration = configuration;
    }

    @Override
    public Neo4JVertex addVertex(Object... keyValues) {
        transaction.readWrite();
        return session.createVertex(keyValues);
    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> graphComputerClass) throws IllegalArgumentException {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    @Override
    public Iterator<Vertex> vertices(@Nonnull @NonNull final Object... vertexIds) {
        transaction.readWrite();
        return session.getOrLoadVertices(this, vertexIds);
    }

    @Override
    public Iterator<Edge> edges(@Nonnull @NonNull final Object... edgeIds) {
        transaction.readWrite();
        return session.getOrLoadEdges(this, edgeIds);
    }

    @Override
    public Transaction tx() {
        return transaction;
    }

    @Override
    public void close() throws Exception {
        if (session.isTxOpen()) {
            throw Transaction.Exceptions.openTransactionsOnClose();
        }
        session.close();
    }

    @Override
    public Variables variables() {
        throw Graph.Exceptions.variablesNotSupported();
    }

    @Override
    public Configuration configuration() {
        return configuration.toApacheConfiguration();
    }

    @Override
    public Features features() {
        return Neo4Features.INSTANCE;
    }
}
