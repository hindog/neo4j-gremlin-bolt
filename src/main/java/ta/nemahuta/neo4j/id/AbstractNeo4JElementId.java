package ta.nemahuta.neo4j.id;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

/**
 * Abstract {@link Neo4JElementId}.
 *
 * @param <T> the type of the actual identifier
 * @author Christian Heike (christian.heike@icloud.com)
 */
@RequiredArgsConstructor
@ToString
@EqualsAndHashCode
public abstract class AbstractNeo4JElementId<T> implements Neo4JElementId<T> {

    @Getter(onMethod = @__(@Override))
    protected final T id;

    @Getter(onMethod = @__(@Override))
    protected final boolean remote;

}
