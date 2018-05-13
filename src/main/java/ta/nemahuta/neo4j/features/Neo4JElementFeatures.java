package ta.nemahuta.neo4j.features;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.FeatureDescriptor;

class Neo4JElementFeatures implements Graph.Features.ElementFeatures {

    @Override
    @FeatureDescriptor(name = FEATURE_USER_SUPPLIED_IDS)
    public boolean supportsUserSuppliedIds() {
        return false;
    }

    @Override
    @FeatureDescriptor(name = FEATURE_STRING_IDS)
    public boolean supportsStringIds() {
        return false;
    }

    @Override
    @FeatureDescriptor(name = FEATURE_UUID_IDS)
    public boolean supportsUuidIds() {
        return false;
    }

    @Override
    @FeatureDescriptor(name = FEATURE_ANY_IDS)
    public boolean supportsAnyIds() {
        return false;
    }

    @Override
    @FeatureDescriptor(name = FEATURE_CUSTOM_IDS)
    public boolean supportsCustomIds() {
        return false;
    }
}
