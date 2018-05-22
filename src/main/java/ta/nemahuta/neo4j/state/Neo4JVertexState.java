package ta.nemahuta.neo4j.state;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

import javax.annotation.Nonnull;

@EqualsAndHashCode
@ToString
public class Neo4JVertexState extends Neo4JElementState {

    @Getter
    protected final ImmutableSet<String> labels;

    public Neo4JVertexState(@Nonnull @NonNull final ImmutableSet<String> labels,
                            @Nonnull final ImmutableMap<String, Object> properties) {
        super(properties);
        this.labels = labels;
    }

    @Override
    public Neo4JVertexState withProperties(@Nonnull @NonNull final ImmutableMap<String, Object> properties) {
        return new Neo4JVertexState(labels, properties);
    }

}
