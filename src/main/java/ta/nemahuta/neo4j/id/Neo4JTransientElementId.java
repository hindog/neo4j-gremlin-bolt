package ta.nemahuta.neo4j.id;

import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Denotes a transient element identifier, which is only temporary.
 *
 * @param <T> the type of the actual identifier.
 * @author Christian Heike (christian.heike@icloud.com)
 */
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class Neo4JTransientElementId<T> extends AbstractNeo4JElementId<T> {

    public Neo4JTransientElementId(final T id) {
        super(id);
    }

    @Override
    public boolean isRemote() {
        return false;
    }

}
