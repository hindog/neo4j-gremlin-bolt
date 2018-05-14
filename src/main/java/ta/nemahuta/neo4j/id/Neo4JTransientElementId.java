package ta.nemahuta.neo4j.id;

/**
 * Denotes a transient element identifier, which is only temporary.
 *
 * @param <T> the type of the actual identifier.
 * @author Christian Heike (christian.heike@icloud.com)
 */
public class Neo4JTransientElementId<T> extends AbstractNeo4JElementId<T> {

    public Neo4JTransientElementId(final T id) {
        super(id, false);
    }

}
