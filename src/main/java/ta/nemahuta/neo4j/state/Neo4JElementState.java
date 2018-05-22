package ta.nemahuta.neo4j.state;

import com.google.common.collect.ImmutableMap;
import lombok.*;
import ta.nemahuta.neo4j.structure.Neo4JElement;

import javax.annotation.Nonnull;

/**
 * Immutable state for a {@link Neo4JElement}. The states includes the id, orLabelsAnd and properties of the element.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
@EqualsAndHashCode
@ToString
@RequiredArgsConstructor
public abstract class Neo4JElementState {

    /**
     * the properties for the element
     */
    @Getter
    @NonNull
    protected final ImmutableMap<String, Object> properties;

    public abstract Neo4JElementState withProperties(@Nonnull ImmutableMap<String, Object> properties);

}
