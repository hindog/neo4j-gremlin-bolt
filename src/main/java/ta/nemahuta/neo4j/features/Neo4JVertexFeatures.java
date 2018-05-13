package ta.nemahuta.neo4j.features;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.FeatureDescriptor;

import javax.annotation.Nonnull;

class Neo4JVertexFeatures extends Neo4JElementFeatures implements Graph.Features.VertexFeatures {

    private final Graph.Features.VertexPropertyFeatures vertexPropertyFeatures = new Neo4JVertexPropertyFeatures();

    @Override
    @Nonnull
    public Graph.Features.VertexPropertyFeatures properties() {
        return vertexPropertyFeatures;
    }

    @Override
    @FeatureDescriptor(name = FEATURE_META_PROPERTIES)
    public boolean supportsMetaProperties() {
        return false;
    }

    @Override
    @FeatureDescriptor(name = FEATURE_MULTI_PROPERTIES)
    public boolean supportsMultiProperties() {
        return false;
    }

    @Override
    @Nonnull
    public VertexProperty.Cardinality getCardinality(final String key) {
        return VertexProperty.Cardinality.single;
    }

}
