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

package ta.nemahuta.neo4j.partition;

import ta.nemahuta.neo4j.query.WherePredicate;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.Set;

/**
 * A partition in the graph, which can be used to separate parts of graphs.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
public interface Neo4JGraphPartition {

    /**
     * Ensure that the source orLabelsAnd include minimum of the partition orLabelsAnd.
     *
     * @param labels the source orLabelsAnd
     * @return the orLabelsAnd from the source including the ones from the partition
     */
    Set<String> ensurePartitionLabelsSet(@Nonnull Iterable<String> labels);

    /**
     * Ensure that the source orLabelsAnd do NOT include the minimum of partition orLabelsAnd.
     *
     * @param labels the source orLabelsAnd
     * @return the orLabelsAnd from the source excluding the ones from the partion
     */
    Set<String> ensurePartitionLabelsNotSet(@Nonnull Iterable<String> labels);

    /**
     * Create a new predicate to match the orLabelsAnd of the partitions for the provided alias in a where clause.
     *
     * @return the {@link Optional} of the {@link WherePredicate} matching the labels
     */
    Optional<WherePredicate> vertexWhereLabelPredicate(@Nonnull String alias);

}
