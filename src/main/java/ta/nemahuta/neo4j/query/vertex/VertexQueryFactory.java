package ta.nemahuta.neo4j.query.vertex;

import ta.nemahuta.neo4j.query.operation.CreatePropertyIndex;
import ta.nemahuta.neo4j.query.operation.DeleteOperation;
import ta.nemahuta.neo4j.query.operation.ReturnIdOperation;
import ta.nemahuta.neo4j.query.operation.UpdateLabelsOperation;
import ta.nemahuta.neo4j.query.operation.UpdatePropertiesOperation;
import ta.nemahuta.neo4j.query.vertex.operation.CreateVertexOperation;
import ta.nemahuta.neo4j.query.vertex.operation.ReturnVertexOperation;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Set;

/**
 * Interface for the factory of predicates and operations for the vertex query.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
public abstract class VertexQueryFactory extends VertexQueryPredicateFactory {

    /**
     * @return a delete operation
     */
    @Nonnull
    public VertexOperation delete() {
        return new DeleteOperation(getAlias());
    }

    /**
     * @return an operation which returns the vertex
     */
    @Nonnull
    public VertexOperation returnVertex() {
        return new ReturnVertexOperation(getAlias());
    }

    /**
     * Set/unset the orLabelsAnd so they match with the current ones.
     *
     * @param committedLabels the committed orLabelsAnd
     * @param currentLabels   the orLabelsAnd which have to be set
     * @return the operation
     */
    @Nonnull
    public VertexOperation labels(@Nonnull final Set<String> committedLabels,
                                  @Nonnull final Set<String> currentLabels) {
        return new UpdateLabelsOperation(committedLabels, getPartition().ensurePartitionLabelsSet(currentLabels), getAlias());
    }


    /**
     * Set/unset the properties so they match with the current ones.
     *
     * @param committedProperties the committed properties
     * @param currentProperties   the properties which are currently set
     * @return the operation
     */
    @Nonnull
    public VertexOperation properties(@Nonnull final Map<String, Object> committedProperties,
                                      @Nonnull final Map<String, Object> currentProperties) {
        return new UpdatePropertiesOperation(committedProperties, currentProperties, getAlias(), getParamNameGenerator().generate("vertexProps"));
    }

    /**
     * Create a vertex using the provided parameters.
     *
     * @param labels     the orLabelsAnd for the vertex
     * @param properties the initial properties
     * @return the operation
     */
    @Nonnull
    public VertexOperation create(@Nonnull final Set<String> labels,
                                  @Nonnull final Map<String, Object> properties) {
        return new CreateVertexOperation(getPartition().ensurePartitionLabelsSet(labels), properties,
                getAlias(), getParamNameGenerator().generate("vertexProps"));
    }

    @Nonnull
    public VertexOperation returnId() {
        return new ReturnIdOperation(getAlias());
    }

    /**
     * Create a new property index for vertices with the provided labels.
     *
     * @param label         the label to be matched for the index
     * @param propertyNames the property names to create an index for
     * @return the operation
     */
    @Nonnull
    public VertexOperation createPropertyIndex(@Nonnull final String label,
                                               @Nonnull final Set<String> propertyNames) {
        return new CreatePropertyIndex(label, propertyNames);
    }

}
