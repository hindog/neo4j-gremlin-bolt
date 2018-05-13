package ta.nemahuta.neo4j.features;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.FeatureDescriptor;

import javax.annotation.Nonnull;

class Neo4JGraphFeatures implements Graph.Features.GraphFeatures {

    private final Graph.Features.VariableFeatures variableFeatures = new Neo4JVariableFeatures();

    @Override
    @Nonnull
    public Graph.Features.VariableFeatures variables() {
        return variableFeatures;
    }

    @Override
    @FeatureDescriptor(name = FEATURE_COMPUTER)
    public boolean supportsComputer() {
        return false;
    }

    @Override
    @FeatureDescriptor(name = FEATURE_THREADED_TRANSACTIONS)
    public boolean supportsThreadedTransactions() {
        return false;
    }
}
