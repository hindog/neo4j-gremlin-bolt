package ta.nemahuta.neo4j.features;

import org.apache.tinkerpop.gremlin.structure.Graph;

import javax.annotation.Nonnull;

class Neo4JEdgeFeatures extends Neo4JElementFeatures implements Graph.Features.EdgeFeatures {

    private final Graph.Features.EdgePropertyFeatures edgePropertyFeatures = new Neo4JEdgePropertyFeatures();

    @Override
    @Nonnull
    public Graph.Features.EdgePropertyFeatures properties() {
        return edgePropertyFeatures;
    }

}
