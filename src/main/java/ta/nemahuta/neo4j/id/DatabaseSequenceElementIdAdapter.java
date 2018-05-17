package ta.nemahuta.neo4j.id;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.types.Entity;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * {@link AbstractNeo4JElementIdAdapter} implementation based on a sequence generator stored in a Neo4J database Node.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
@Slf4j
public class DatabaseSequenceElementIdAdapter extends AbstractNeo4JElementIdAdapter {

    public static final String DEFAULT_PROPERTY_NAME = "id";
    public static final String DEFAULT_SEQUENCE_NODE_LABEL = "UniqueIdentifierGenerator";
    public static final long DEFAULT_POOL_SIZE = 100l;
    public static final String PARAM_POOL_SIZE = "poolSize";

    private final Driver driver;
    private final String idFieldName;
    private final long poolSize;

    private final AtomicLong currentIdentifier = new AtomicLong(0L);
    private final AtomicLong maximumIdentifier = new AtomicLong(0L);
    private final ReentrantReadWriteLock lockProvider = new ReentrantReadWriteLock();
    private final String query;
    private final Map<String, Object> parameters;


    public DatabaseSequenceElementIdAdapter(@NonNull @Nonnull final Driver driver) {
        this(driver, DEFAULT_POOL_SIZE, DEFAULT_PROPERTY_NAME, DEFAULT_SEQUENCE_NODE_LABEL);
    }

    public DatabaseSequenceElementIdAdapter(@Nonnull @NonNull final Driver driver,
                                            final long poolSize,
                                            @Nonnull @NonNull final String idFieldName,
                                            @Nonnull @NonNull final String sequenceNodeLabel) {
        this.driver = driver;
        if (poolSize <= 0l) {
            throw new IllegalArgumentException("Pool size should be greater than zero.");
        }
        this.poolSize = poolSize;
        this.idFieldName = idFieldName;
        this.query = "MERGE (g:`" + sequenceNodeLabel + "`) ON CREATE SET g.nextId = 1 ON MATCH SET g.nextId = g.nextId + {" + PARAM_POOL_SIZE + "} RETURN g.nextId";
        this.parameters = Collections.singletonMap(PARAM_POOL_SIZE, poolSize);
    }

    @Override
    @Nonnull
    public Optional<String> propertyName() {
        return Optional.of(idFieldName);
    }

    @Override
    @Nonnull
    public Neo4JElementId<Long> retrieveId(@Nonnull @NonNull final Entity entity) {
        return new Neo4JPersistentElementId<>(entity.get(idFieldName).asLong());
    }

    @Override
    @Nonnull
    public Neo4JElementId<Long> generate() {
        return new Neo4JPersistentElementId<>(generateId());
    }

    /**
     * @return a new identifier from the batch/graph
     */
    protected long generateId() {
        final long max = maximumIdentifier.get();
        final long identifier = currentIdentifier.incrementAndGet();
        return identifier <= max ? identifier : retrieveNewIdBatch(identifier, max);
    }

    /**
     * Retrieves a new identifier batch from the graph.
     *
     * @param currentMax the current maximum identifier
     * @return the new identifier retrieved
     */
    protected long retrieveNewIdBatch(final long identifier, final long currentMax) {
        final ReentrantReadWriteLock.WriteLock writeLock = lockProvider.writeLock();
        writeLock.lock();
        try {
            long result;
            long max;
            do {
                try (Session session = driver.session()) {
                    try (Transaction transaction = session.beginTransaction()) {
                        final StatementResult stmtResult = transaction.run(new Statement(query, parameters));
                        if (stmtResult.hasNext()) {
                            max = stmtResult.next().get(0).asLong();
                            if (max < currentMax) {
                                throw new IllegalStateException("The generated identifier " + max + " is lower than the currently stored maximum: " + currentMax);
                            }
                            result = max - (poolSize - 1);
                        } else {
                            throw new IllegalStateException("Could not retrieve an identifier batch from the graph.");
                        }
                        transaction.success();
                    }
                    log.debug("Pool size provided from generator: {}...{}", result, max);
                }
            } while (identifier > max);
            this.currentIdentifier.set(result);
            this.maximumIdentifier.set(max);
            return result;
        } finally {
            writeLock.unlock();
        }
    }

}