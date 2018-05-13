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

package ta.nemahuta.neo4j.features;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import javax.annotation.Nonnull;

/**
 * @author Rogelio J. Baucells
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class Neo4Features implements Graph.Features {

    public static final Neo4Features INSTANCE = new Neo4Features();

    private final GraphFeatures graphFeatures = new Neo4JGraphFeatures();
    private final VertexFeatures vertexFeatures = new Neo4JVertexFeatures();
    private final EdgeFeatures edgeFeatures = new Neo4JEdgeFeatures();

    @Override
    @Nonnull
    public GraphFeatures graph() {
        return graphFeatures;
    }

    @Override
    @Nonnull
    public VertexFeatures vertex() {
        return vertexFeatures;
    }

    @Override
    @Nonnull
    public EdgeFeatures edge() {
        return edgeFeatures;
    }

    @Override
    @Nonnull
    public String toString() {
        return StringFactory.featureString(this);
    }
}