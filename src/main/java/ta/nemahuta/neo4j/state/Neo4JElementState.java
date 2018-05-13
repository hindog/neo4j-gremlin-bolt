package ta.nemahuta.neo4j.state;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.ToString;
import lombok.experimental.Wither;
import ta.nemahuta.neo4j.id.Neo4JElementId;
import ta.nemahuta.neo4j.structure.Neo4JElement;

import javax.annotation.Nonnull;

/**
 * Immutable state for a {@link Neo4JElement}. The states includes the id, orLabelsAnd and properties of the element.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
@EqualsAndHashCode
@ToString
@Wither
public class Neo4JElementState {

    /**
     * the element id
     */
    public final Neo4JElementId<?> id;
    /**
     * the orLabelsAnd for the element
     */
    public final ImmutableSet<String> labels;
    /**
     * the properties for the element
     */
    public final ImmutableMap<String, PropertyValue<?>> properties;

    /**
     * Construct a new state using the provided parameters.
     *
     * @param id         the id of the element
     * @param labels     the orLabelsAnd of the element
     * @param properties the properties of the elemnt
     */
    public Neo4JElementState(@Nonnull @NonNull final Neo4JElementId<?> id,
                             @Nonnull @NonNull final ImmutableSet<String> labels,
                             @Nonnull @NonNull final ImmutableMap<String, PropertyValue<?>> properties) {
        this.id = id;
        this.labels = labels;
        this.properties = properties;
    }

}
