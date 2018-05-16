package ta.nemahuta.neo4j.query;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import org.apache.tinkerpop.gremlin.structure.Direction;
import ta.nemahuta.neo4j.structure.Neo4JElement;
import ta.nemahuta.neo4j.structure.Neo4JProperty;

import javax.annotation.Nonnull;
import java.util.*;

/**
 * Utilities being used to create queries.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class QueryUtils {

    /**
     * Append the orLabelsAnd and the readPartition orLabelsAnd to the query builder.
     *
     * @param sb     the query builder
     * @param labels the orLabelsAnd to be appended
     */
    public static void appendLabels(@Nonnull @NonNull final StringBuilder sb,
                                    @Nonnull @NonNull final Set<String> labels) {
        labels.forEach(label -> appendLabel(sb, label));
    }


    /**
     * Compute a map of properties from the currently set ones
     *
     * @param committedProperties the currently committed properties
     * @param currentProperties   the properties to be committed
     * @return a map of properties to be added as a parameter
     */
    @Nonnull
    public static Map<String, Object> computeProperties(@Nonnull @NonNull final Map<String, ? extends Neo4JProperty<? extends Neo4JElement, ?>> committedProperties,
                                                        @Nonnull @NonNull final Map<String, ? extends Neo4JProperty<? extends Neo4JElement, ?>> currentProperties) {
        final Map<String, Object> properties = new HashMap<>();
        // process current properties
        currentProperties.entrySet().stream()
                .filter(e -> !Objects.equals(e.getValue().value(),
                        Optional.ofNullable(committedProperties.get(e.getKey())).map(Neo4JProperty::value).orElse(null))
                )
                .forEach(e -> {
                    if (!Objects.equals(e.getValue(), committedProperties.get(e.getKey()))) {
                        properties.put(e.getKey(), e.getValue().value());
                    }
                });
        // removed properties are computed by subtracting the current properties from the committed ones and setting them to null
        committedProperties.keySet().stream()
                .filter(k -> !currentProperties.keySet().contains(k))
                .forEach(k -> properties.put(k, null));
        return properties;
    }


    /**
     * Append a label to the query builder.
     *
     * @param sb    the query builder
     * @param label the label to be appended
     */
    private static void appendLabel(@Nonnull @NonNull final StringBuilder sb,
                                    @Nonnull @NonNull final String label) {
        sb.append(":`").append(label).append("`");
    }

    /**
     * Append the relation prefix to a {@link StringBuilder}
     *
     * @param direction the direction to be used
     * @param sb        the target to append to
     */
    public static void appendRelationStart(@Nonnull @NonNull final Direction direction,
                                           @Nonnull @NonNull final StringBuilder sb) {
        sb.append(directionString(direction, true));
    }

    /**
     * Append the relation suffix to a {@link StringBuilder}
     *
     * @param direction the direction to be used
     * @param sb        the target to append to
     */
    public static void appendRelationEnd(@Nonnull @NonNull final Direction direction,
                                         @Nonnull @NonNull final StringBuilder sb) {
        sb.append(directionString(direction, false));
    }

    /**
     * Get the relation prefix/suffix for a direction.
     *
     * @param direction the direction to be used
     * @param start     {@code true} if the relation prefix should be provided, {@code false} if to use the suffix
     * @return the relation prefix/suffix as requested
     */
    @Nonnull
    private static String directionString(@Nonnull @NonNull final Direction direction,
                                          final boolean start) {
        switch (direction) {
            case IN:
                return start ? "<-[" : "]-";
            case OUT:
                return start ? "-[" : "]->";
            case BOTH:
                return start ? "-[" : "]-";
            default:
                throw new IllegalArgumentException("Cannot handle direction: " + direction);
        }
    }
}
