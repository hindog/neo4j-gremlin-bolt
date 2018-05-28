package ta.nemahuta.neo4j.state;

import com.google.common.collect.ImmutableMap;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import javax.annotation.Nonnull;

@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class Neo4JEdgeState extends Neo4JElementState {

    @Getter
    private final String label;

    @Getter
    private final long inVertexId, outVertexId;

    public Neo4JEdgeState(@Nonnull final String label,
                          @Nonnull final ImmutableMap<String, Object> properties,
                          final long inVertexId,
                          final long outVertexId) {
        super(properties);
        this.label = label;
        this.inVertexId = inVertexId;
        this.outVertexId = outVertexId;
    }

    @Override
    public Neo4JEdgeState withProperties(@Nonnull final ImmutableMap<String, Object> properties) {
        return new Neo4JEdgeState(label, properties, inVertexId, outVertexId);
    }

}
