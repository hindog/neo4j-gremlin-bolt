package ta.nemahuta.neo4j.query.vertex;

import lombok.NonNull;
import ta.nemahuta.neo4j.id.Neo4JElementId;
import ta.nemahuta.neo4j.query.operation.DeleteOperation;
import ta.nemahuta.neo4j.query.operation.ReturnIdOperation;
import ta.nemahuta.neo4j.query.operation.UpdateLabelsOperation;
import ta.nemahuta.neo4j.query.operation.UpdatePropertiesOperation;
import ta.nemahuta.neo4j.query.vertex.operation.CreateVertexOperation;
import ta.nemahuta.neo4j.query.vertex.operation.ReturnVertexOperation;
import ta.nemahuta.neo4j.state.PropertyValue;

import javax.annotation.Nonnull;
import java.util.Collections;
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
     * Set the provided orLabelsAnd.
     *
     * @param labels the orLabelsAnd to be set
     * @return the operation
     */
    @Nonnull
    public VertexOperation labels(@Nonnull @NonNull final Set<String> labels) {
        return labels(Collections.emptySet(), labels);
    }

    /**
     * Set/unset the orLabelsAnd so they match with the current ones.
     *
     * @param committedLabels the committed orLabelsAnd
     * @param currentLabels   the orLabelsAnd which have to be set
     * @return the operation
     */
    @Nonnull
    public VertexOperation labels(@Nonnull @NonNull final Set<String> committedLabels,
                                  @Nonnull @NonNull final Set<String> currentLabels) {
        return new UpdateLabelsOperation(committedLabels, currentLabels, getAlias());
    }

    /**
     * Set the provided properties.
     *
     * @param properties the properties to be set
     * @return the operation
     */
    @Nonnull
    public VertexOperation properties(@Nonnull @NonNull final Map<String, PropertyValue<?>> properties) {
        return properties(Collections.emptyMap(), properties);
    }

    /**
     * Set/unset the properties so they match with the current ones.
     *
     * @param committedProperties the committed properties
     * @param currentProperties   the properties which are currently set
     */
    @Nonnull
    public VertexOperation properties(@Nonnull @NonNull final Map<String, PropertyValue<?>> committedProperties,
                                      @Nonnull @NonNull final Map<String, PropertyValue<?>> currentProperties) {
        return new UpdatePropertiesOperation(committedProperties, currentProperties, getAlias(), getParamNameGenerator().generate("vertexProps"));
    }

    /**
     * Create a vertex using the provided parameters.
     *
     * @param id         the id of the vertex
     * @param labels     the orLabelsAnd for the vertex
     * @param properties the initial properties
     * @return the operation
     */
    @Nonnull
    public VertexOperation create(@Nonnull @NonNull final Neo4JElementId<?> id,
                                  @Nonnull @NonNull final Set<String> labels,
                                  @Nonnull @NonNull final Map<String, PropertyValue<?>> properties) {
        return new CreateVertexOperation(getIdAdapter(), id, labels, properties, getParamNameGenerator().generate("vertexProps"), getAlias());
    }

    @Nonnull
    public VertexOperation returnId() {
        return new ReturnIdOperation(getAlias(), getIdAdapter());
    }

}
