package ta.nemahuta.neo4j.query;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.tinkerpop.gremlin.structure.Direction;

import javax.annotation.Nonnull;
import java.util.Set;

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
    public static void appendLabels(@Nonnull final StringBuilder sb,
                                    @Nonnull final Set<String> labels) {
        labels.forEach(label -> appendLabel(sb, label));
    }

    /**
     * Append a label to the query builder.
     *
     * @param sb    the query builder
     * @param label the label to be appended
     */
    private static void appendLabel(@Nonnull final StringBuilder sb,
                                    @Nonnull final String label) {
        sb.append(":`").append(label).append("`");
    }

    /**
     * Append the relation prefix to a {@link StringBuilder}
     *
     * @param direction the direction to be used
     * @param sb        the target to append to
     */
    public static void appendRelationStart(@Nonnull final Direction direction,
                                           @Nonnull final StringBuilder sb) {
        sb.append(directionString(direction, true));
    }

    /**
     * Append the relation suffix to a {@link StringBuilder}
     *
     * @param direction the direction to be used
     * @param sb        the target to append to
     */
    public static void appendRelationEnd(@Nonnull final Direction direction,
                                         @Nonnull final StringBuilder sb) {
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
    private static String directionString(@Nonnull final Direction direction,
                                          final boolean start) {
        switch (direction) {
            case IN:
                return start ? "<-[" : "]-";
            case OUT:
                return start ? "-[" : "]->";
            case BOTH:
            default:
                return start ? "-[" : "]-";
        }
    }
}
