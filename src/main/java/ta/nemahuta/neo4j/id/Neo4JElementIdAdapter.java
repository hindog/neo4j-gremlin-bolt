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

import org.neo4j.driver.v1.types.Entity;
import ta.nemahuta.neo4j.id.Neo4JElementId;
import ta.nemahuta.neo4j.id.Neo4JElementIdGenerator;

/**
 * Interface for the adapter of {@link Neo4JElementId}s.
 *
 * @author Christian Heike
 */
public interface Neo4JElementIdAdapter<T> extends Neo4JElementIdGenerator<T> {

    /**
     * @return the property name used to store the identifier of an {@link Entity}.
     */
    String propertyName();

    /**
     * Retrieves the {@link Neo4JElementId} from an {@link Entity}.
     *
     * @param entity the source {@link Entity}
     * @return the {@link Neo4JElementId} retrieved from the entity
     */
    Neo4JElementId<T> retrieveId(Entity entity);

    /**
     * Converts the provided {@link Object} to a {@link Neo4JElementId}.
     *
     * @param id the object to be converted
     * @return the {@link Neo4JElementId} from the object
     */
    Neo4JElementId<T> convert(Object id);
}
