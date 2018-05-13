package ta.nemahuta.neo4j.session.scope;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import ta.nemahuta.neo4j.session.Neo4JElementScope;
import ta.nemahuta.neo4j.session.SessionScope;
import ta.nemahuta.neo4j.structure.Neo4JVertex;

import javax.annotation.Nonnull;

/**
 * Default implementation of the {@link SessionScope}.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
@RequiredArgsConstructor
public class DefaultSessionScope implements SessionScope {

    @Getter(onMethod = @__({@Override, @Nonnull}))
    private final Neo4JElementScope<Neo4JVertex> vertexScope;

    @Getter(onMethod = @__({@Override, @Nonnull}))
    private final Neo4JEdgeScope edgeScope;

    @Override
    public void commit() {
        vertexScope.commit();
        edgeScope.commit();
    }

    @Override
    public void rollback() {
        edgeScope.rollback();
        vertexScope.rollback();
    }

}
