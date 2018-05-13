/*
 *  Copyright 2016 SteelBridge Laboratories, LLC.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  For more information: http://steelbridgelabs.com
 */

package ta.nemahuta.neo4j.id;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.types.Entity;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@link AbstractNeo4JElementIdAdapter} implementation based on a sequence generator stored in a Neo4J database Node.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
@Slf4j
public class DatabaseSequenceElementIdAdapter extends AbstractNeo4JElementIdAdapter {

    public static final String DefaultIdFieldName = "id";
    public static final String DefaultSequenceNodeLabel = "UniqueIdentifierGenerator";
    public static final long DefaultPoolSize = 1000;

    private final Driver driver;
    private final String idFieldName;
    private final String sequenceNodeLabel;
    private final long poolSize;
    private final AtomicLong atomicLong = new AtomicLong(0L);
    private final Object monitor = new Object();

    private AtomicLong maximum = new AtomicLong(0L);

    public DatabaseSequenceElementIdAdapter(@NonNull @Nonnull final Driver driver) {
        this(driver, DefaultPoolSize, DefaultIdFieldName, DefaultSequenceNodeLabel);
    }

    public DatabaseSequenceElementIdAdapter(@Nonnull @NonNull final Driver driver,
                                            final long poolSize,
                                            @Nonnull @NonNull final String idFieldName,
                                            @Nonnull @NonNull final String sequenceNodeLabel) {
        this.driver = driver;
        this.poolSize = poolSize;
        this.idFieldName = idFieldName;
        this.sequenceNodeLabel = sequenceNodeLabel;
    }

    /**
     * Gets the field name used for {@link Entity} identifier.
     *
     * @return The field name used for {@link Entity} identifier or <code>null</code> if not using field for identifier.
     */
    @Override
    public String propertyName() {
        return idFieldName;
    }

    /**
     * Gets the identifier value from a neo4j {@link Entity}.
     *
     * @param entity The neo4j {@link Entity}.
     * @return The neo4j {@link Entity} identifier.
     */
    @Override
    public Neo4JElementId<Long> retrieveId(Entity entity) {
        Objects.requireNonNull(entity, "entity cannot be null");
        // return property value
        return new Neo4JPersistentElementId<>(entity.get(idFieldName).asLong());
    }

    /**
     * Generates a new identifier value. This {@link Neo4JElementIdAdapter} will fetch a pool of identifiers
     * from a Neo4J database Node.
     *
     * @return A unique identifier within the database sequence generator.
     */
    @Override
    public Neo4JElementId<Long> generate() {
        // retrieveId maximum identifier we can use (before obtaining new identifier to make sure it is in the current pool)
        long max = maximum.get();
        // generate new identifier
        long identifier = atomicLong.incrementAndGet();
        // check we need to obtain new identifier pool (identifier is out of range for current pool)
        if (identifier > max) {
            // loop until we retrieveId an identifier value
            do {
                // log information
                if (log.isDebugEnabled())
                    log.debug("About to request a pool of identifiers from database, maximum id: {}", max);
                // make sure only one thread gets a new range of identifiers
                synchronized (monitor) {
                    // update maximum number in pool, do not switch the next two statements (in case another thread was executing the synchronized block while the current thread was waiting)
                    max = maximum.get();
                    identifier = atomicLong.incrementAndGet();
                    // verify a new identifier is needed (compare it with current maximum)
                    if (identifier >= max) {
                        // create database session
                        try (Session session = driver.session()) {
                            // create transaction
                            try (Transaction transaction = session.beginTransaction()) {
                                // create cypher command, reserve poolSize identifiers
                                Statement statement = new Statement("MERGE (g:`" + sequenceNodeLabel + "`) ON CREATE SET g.nextId = 1 ON MATCH SET g.nextId = g.nextId + {poolSize} RETURN g.nextId", Collections.singletonMap("poolSize", poolSize));
                                // execute statement
                                StatementResult result = transaction.run(statement);
                                // process result
                                if (result.hasNext()) {
                                    // retrieveId record
                                    Record record = result.next();
                                    // retrieveId nextId value
                                    long nextId = record.get(0).asLong();
                                    // set value for next identifier (do not switch the next two statements!)
                                    atomicLong.set(nextId - poolSize);
                                    maximum.set(nextId);
                                }
                                // commit
                                transaction.success();
                            }
                        }
                        // update maximum number in pool
                        max = maximum.get();
                        // retrieveId a new identifier
                        identifier = atomicLong.incrementAndGet();
                        // log information
                        if (log.isDebugEnabled())
                            log.debug("Requested new pool of identifiers from database, current id: {}, maximum id: {}", identifier, max);
                    } else if (log.isDebugEnabled())
                        log.debug("No need to request pool of identifiers, current id: {}, maximum id: {}", identifier, max);
                }
            }
            while (identifier > max);
        } else if (log.isDebugEnabled())
            log.debug("Current identifier: {}", identifier);
        // return identifier
        return new Neo4JPersistentElementId<>(identifier);
    }

}