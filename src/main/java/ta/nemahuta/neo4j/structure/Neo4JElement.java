package ta.nemahuta.neo4j.structure;

import lombok.NonNull;
import lombok.ToString;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import ta.nemahuta.neo4j.id.Neo4JElementId;
import ta.nemahuta.neo4j.state.LocalAndRemoteStateHolder;
import ta.nemahuta.neo4j.state.Neo4JElementState;
import ta.nemahuta.neo4j.state.StateHolder;
import ta.nemahuta.neo4j.state.SyncState;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.function.Function;

/**
 * Abstract implementation of an {@link Element} for Neo4J.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
@ToString
public abstract class Neo4JElement implements Element {

    public final LocalAndRemoteStateHolder<Neo4JElementState> state;
    protected final Neo4JGraph graph;

    /**
     * Create a new element for a certain graph with an initial state.
     *
     * @param graph        the graph the element is bound to
     * @param initialState the initial state of the element
     */
    protected Neo4JElement(@Nonnull @NonNull final Neo4JGraph graph,
                           @Nonnull @NonNull final StateHolder<Neo4JElementState> initialState) {
        this.graph = graph;
        this.state = new LocalAndRemoteStateHolder<>(Objects.requireNonNull(initialState, "initial state may not be null"));
    }

    <R> R currentState(final Function<Neo4JElementState, R> function) {
        return state.current(s -> function.apply(s.state));
    }

    public boolean isNotDiscarded() {
        return !SyncState.DISCARDED.equals(state.getCurrentSyncState());
    }

    public void modify(final Function<Neo4JElementState, Neo4JElementState> update) {
        state.modify(update);
    }

    public void delete() {
        state.delete();
    }

    @Override
    public Neo4JElementId<?> id() {
        return state.current(s -> s.state.id);
    }

    @Override
    public String label() {
        // orLabelsAnd separated by "::"
        return state.current(s -> String.join("::", s.state.labels));
    }

    @Override
    public Graph graph() {
        return graph;
    }

    @Override
    public void remove() {
        state.delete();
    }

    @Override
    public int hashCode() {
        return currentState(s -> new HashCodeBuilder().append(getClass().getName()).appendSuper(s.hashCode()).toHashCode());
    }

}
