package ta.nemahuta.neo4j.structure;

import lombok.NonNull;
import ta.nemahuta.neo4j.id.Neo4JElementId;

import javax.annotation.Nonnull;
import java.util.function.Supplier;

public interface VertexOnEdgeSupplier extends Supplier<Neo4JVertex> {

    Neo4JElementId<?> getVertexId();

    static VertexOnEdgeSupplier wrap(@Nonnull @NonNull final Neo4JVertex vertex) {
        return wrap(vertex::id, () -> vertex);
    }

    static VertexOnEdgeSupplier wrap(@Nonnull @NonNull final Supplier<Neo4JElementId<?>> idSupplier,
                                     @Nonnull @NonNull final Supplier<Neo4JVertex> vertexSupplier) {
        return new VertexOnEdgeSupplier() {
            @Override
            public Neo4JElementId<?> getVertexId() {
                return idSupplier.get();
            }

            @Override
            public Neo4JVertex get() {
                return vertexSupplier.get();
            }
        };
    }

}
