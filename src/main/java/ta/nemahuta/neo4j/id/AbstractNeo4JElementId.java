package ta.nemahuta.neo4j.id;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Abstract {@link Neo4JElementId}.
 *
 * @param <T> the type of the actual identifier
 * @author Christian Heike (christian.heike@icloud.com)
 */
@RequiredArgsConstructor
public abstract class AbstractNeo4JElementId<T> implements Neo4JElementId<T> {

    @Getter(onMethod = @__(@Override))
    protected final T id;

}
