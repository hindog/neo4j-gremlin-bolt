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

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;

/**
 * @author Rogelio J. Baucells
 */
public class NoReadPartitionWhileContainsVertexTest {

    @Test
    public void givenVertexWithLabelsShouldReturnTrue() {
        // arrange
        Neo4JGraphPartition partition = new NoGraphPartition();
        // act
        boolean result = partition.containsVertex(new HashSet<>(Arrays.asList("l1", "l2", "l3")));
        // assert
        Assert.assertTrue("Failed to detect vertex orLabelsAnd are in readPartition", result);
    }
}
