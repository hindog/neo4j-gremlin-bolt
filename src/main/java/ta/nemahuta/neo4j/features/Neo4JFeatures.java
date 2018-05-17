package ta.nemahuta.neo4j.features;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import javax.annotation.Nonnull;

/**
 * @author Rogelio J. Baucells
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class Neo4JFeatures implements Graph.Features {

    public static final Neo4JFeatures INSTANCE = new Neo4JFeatures();

    private final GraphFeatures graphFeatures = new Neo4JGraphFeatures();
    private final VertexFeatures vertexFeatures = new Neo4JVertexFeatures();
    private final EdgeFeatures edgeFeatures = new Neo4JEdgeFeatures();

    @Override
    @Nonnull
    public GraphFeatures graph() {
        return graphFeatures;
    }

    @Override
    @Nonnull
    public VertexFeatures vertex() {
        return vertexFeatures;
    }

    @Override
    @Nonnull
    public EdgeFeatures edge() {
        return edgeFeatures;
    }

    @Override
    @Nonnull
    public String toString() {
        return StringFactory.featureString(this);
    }
}