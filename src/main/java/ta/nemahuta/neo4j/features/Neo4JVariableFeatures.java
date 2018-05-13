package ta.nemahuta.neo4j.features;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.FeatureDescriptor;

class Neo4JVariableFeatures implements Graph.Features.VariableFeatures {

    @Override
    @FeatureDescriptor(name = FEATURE_BOOLEAN_VALUES)
    public boolean supportsBooleanValues() {
        return false;
    }

    @Override
    @FeatureDescriptor(name = FEATURE_BYTE_VALUES)
    public boolean supportsByteValues() {
        return false;
    }

    @Override
    @FeatureDescriptor(name = FEATURE_DOUBLE_VALUES)
    public boolean supportsDoubleValues() {
        return false;
    }

    @Override
    @FeatureDescriptor(name = FEATURE_FLOAT_VALUES)
    public boolean supportsFloatValues() {
        return false;
    }

    @Override
    @FeatureDescriptor(name = FEATURE_INTEGER_VALUES)
    public boolean supportsIntegerValues() {
        return false;
    }

    @Override
    @FeatureDescriptor(name = FEATURE_LONG_VALUES)
    public boolean supportsLongValues() {
        return false;
    }

    @Override
    @FeatureDescriptor(name = FEATURE_MAP_VALUES)
    public boolean supportsMapValues() {
        return false;
    }

    @Override
    @FeatureDescriptor(name = FEATURE_MIXED_LIST_VALUES)
    public boolean supportsMixedListValues() {
        return false;
    }

    @Override
    @FeatureDescriptor(name = FEATURE_BOOLEAN_ARRAY_VALUES)
    public boolean supportsBooleanArrayValues() {
        return false;
    }

    @Override
    @FeatureDescriptor(name = FEATURE_BYTE_ARRAY_VALUES)
    public boolean supportsByteArrayValues() {
        return false;
    }

    @Override
    @FeatureDescriptor(name = FEATURE_DOUBLE_ARRAY_VALUES)
    public boolean supportsDoubleArrayValues() {
        return false;
    }

    @Override
    @FeatureDescriptor(name = FEATURE_FLOAT_ARRAY_VALUES)
    public boolean supportsFloatArrayValues() {
        return false;
    }

    @Override
    @FeatureDescriptor(name = FEATURE_INTEGER_ARRAY_VALUES)
    public boolean supportsIntegerArrayValues() {
        return false;
    }

    @Override
    @FeatureDescriptor(name = FEATURE_STRING_ARRAY_VALUES)
    public boolean supportsStringArrayValues() {
        return false;
    }

    @Override
    @FeatureDescriptor(name = FEATURE_LONG_ARRAY_VALUES)
    public boolean supportsLongArrayValues() {
        return false;
    }

    @Override
    @FeatureDescriptor(name = FEATURE_SERIALIZABLE_VALUES)
    public boolean supportsSerializableValues() {
        return false;
    }

    @Override
    @FeatureDescriptor(name = FEATURE_STRING_VALUES)
    public boolean supportsStringValues() {
        return false;
    }

    @Override
    @FeatureDescriptor(name = FEATURE_UNIFORM_LIST_VALUES)
    public boolean supportsUniformListValues() {
        return false;
    }
}
