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
import org.neo4j.driver.v1.types.Entity;

import javax.annotation.Nonnull;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@link AbstractNeo4JElementIdAdapter} implementation based which will used generated ids until the elements have been persisted.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
public class Neo4JNativeElementIdAdapter extends AbstractNeo4JElementIdAdapter {

    /**
     * The id generator for the transient ids
     */
    private final AtomicLong curId = new AtomicLong(Long.MAX_VALUE);

    /**
     * Gets the field name used for {@link Entity} identifier.
     *
     * @return The field name used for {@link Entity} identifier or <code>null</code> if not using field for identifier.
     */
    @Override
    public String propertyName() {
        return "id";
    }

    /**
     * Gets the identifier value from a neo4j {@link Entity}.
     *
     * @param entity The neo4j {@link Entity}.
     * @return The neo4j {@link Entity} identifier.
     */
    @Override
    @Nonnull
    public Neo4JElementId<Long> retrieveId(@Nonnull @NonNull final Entity entity) {
        return new Neo4JPersistentElementId<>(entity.id());
    }

    /**
     * Generates a new identifier value. This {@link Neo4JElementIdAdapter} will fetch a pool of identifiers
     * from a Neo4J database Node.
     *
     * @return A unique identifier within the database sequence generator.
     */
    @Override
    @Nonnull
    public Neo4JElementId<Long> generate() {
        return new Neo4JTransientElementId<>(curId.addAndGet(1l));
    }

}
