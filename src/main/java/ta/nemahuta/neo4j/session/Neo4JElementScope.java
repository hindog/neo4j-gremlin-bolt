package ta.nemahuta.neo4j.session;

import ta.nemahuta.neo4j.id.Neo4JElementId;
import ta.nemahuta.neo4j.id.Neo4JElementIdAdapter;
import ta.nemahuta.neo4j.id.Neo4JElementIdGenerator;
import ta.nemahuta.neo4j.partition.Neo4JGraphPartition;
import ta.nemahuta.neo4j.structure.Neo4JElement;
import ta.nemahuta.neo4j.structure.Neo4JGraph;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.stream.Stream;

/**
 * Interface for an scope of {@link Neo4JElement}s.
 *
 * @param <T> the type of the elements
 * @author Christian Heike (christian.heike@icloud.com)
 */
public interface Neo4JElementScope<T extends Neo4JElement> extends RollbackAndCommit {

    /**
     * @return the identifier provider for the scope
     */
    @Nonnull
    Neo4JElementIdAdapter<?> getIdAdapter();


    @Nonnull
    Neo4JGraphPartition getReadPartition();

    /**
     * @return the identifier generator for the properties of the elements
     */
    @Nonnull
    Neo4JElementIdGenerator<?> getPropertyIdGenerator();

    /**
     * Add the element to the scope.
     *
     * @param element the element to be added
     */
    void add(@Nonnull T element);

    /**
     * Get or load the elements with the provided ids from/to the scope.
     *
     * @param graph the graph the elements should be loaded for
     * @param ids   the identifiers of the elements
     * @return an iterator of the ids of the elements
     */
    Stream<T> getOrLoad(@Nonnull Neo4JGraph graph,
                        @Nonnull Iterator<? extends Neo4JElementId<?>> ids);

    /**
     * Get or load the elements with one of the provided orLabelsAnd from/to the scope.
     *
     * @param graph  the graph the elements should be loaded for
     * @param labels the {@link Iterable} of orLabelsAnd of which one has to match
     * @return an iterator of the ids of the elements
     */
    Stream<T> getOrLoadLabelIn(@Nonnull Neo4JGraph graph,
                               @Nonnull Iterable<String> labels);

    /**
     * Flushes the scope
     */
    void flush();
}
