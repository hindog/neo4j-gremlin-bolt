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

import java.util.Set;

/**
 * @author Rogelio J. Baucells
 */
public class AnyLabelReadPartitionWhileVertexMatchPatternLabelsTest {

    @Test
    public void givenPartitionWithMultipleLabelsShouldReturnVertexMatchPatternLabels() {
        // arrange
        Neo4JGraphPartition partition = new AnyLabelGraphPartition("l1", "l2", "l3");
        // act
        Set<String> result = partition.vertexMatchPatternLabels();
        // assert
        Assert.assertNotNull("Invalid vertex match pattern orLabelsAnd", result);
        Assert.assertTrue("Failed to generate vertex match pattern orLabelsAnd", result.isEmpty());
    }

    @Test
    public void givenPartitionWithOneLabelShouldReturnEmptySet() {
        // arrange
        Neo4JGraphPartition partition = new AnyLabelGraphPartition("l1");
        // act
        Set<String> result = partition.vertexMatchPatternLabels();
        // assert
        Assert.assertNotNull("Invalid vertex match pattern orLabelsAnd", result);
        Assert.assertTrue("Failed to generate vertex match pattern orLabelsAnd", !result.isEmpty());
    }
}
