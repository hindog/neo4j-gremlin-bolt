package ta.nemahuta.neo4j.query;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.neo4j.driver.v1.Statement;
import ta.nemahuta.neo4j.id.Neo4JElementIdAdapter;
import ta.nemahuta.neo4j.partition.Neo4JGraphPartition;

import javax.annotation.Nonnull;
import java.util.*;

/**
 * Abstract implementation of a {@link StatementBuilder}.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
@RequiredArgsConstructor
public abstract class AbstractQueryBuilder implements StatementBuilder {

    /**
     * the adapter for the {@link ta.nemahuta.neo4j.id.Neo4JElementId}s
     */
    @NonNull
    protected final Neo4JElementIdAdapter<?> idAdapter;
    /**
     * the partition for the statement
     */
    @NonNull
    protected final Neo4JGraphPartition partition;

    @Getter(value = AccessLevel.PROTECTED)
    private MatchPredicate match;
    @Getter(value = AccessLevel.PROTECTED)
    private WherePredicate where;
    @Getter(value = AccessLevel.PROTECTED)
    private final List<Operation> operations = new ArrayList<>();

    /**
     * Set the {@link MatchPredicate} for the MATCH clause.
     *
     * @param match the match predicate to be set
     */
    protected void setMatch(@Nonnull @NonNull final MatchPredicate match) {
        this.match = match;
    }

    /**
     * Set the {@link WherePredicate} for the WHERE clause.
     *
     * @param where the where predicate to be set.
     */
    protected void setWhere(@Nonnull @NonNull final WherePredicate where) {
        this.where = where;
    }

    /**
     * Add an {@link Operation} to be issued.
     *
     * @param operation the operation to be added
     */
    protected void addOperation(@Nonnull @NonNull final Operation operation) {
        this.operations.add(operation);
    }

    @Override
    @Nonnull
    public Optional<Statement> build() {
        final StringBuilder sb = new StringBuilder();
        final Map<String, Object> parameters = new HashMap<>();
        Optional.ofNullable(getMatch()).ifPresent(m -> {
            sb.append("\nMATCH ");
            m.append(sb, parameters);
        });
        Optional.ofNullable(getWhere()).ifPresent(w -> {
            sb.append("\nWHERE ");
            w.append(sb, parameters);
        });
        boolean needsOperation = false;
        for (Operation operation : operations) {
            if (operation.isNeedsStatement()) {
                operation.append(sb, parameters);
                needsOperation = true;
            }
        }
        return needsOperation ? Optional.of(new Statement(sb.toString().trim(), parameters)) : Optional.empty();
    }


}
