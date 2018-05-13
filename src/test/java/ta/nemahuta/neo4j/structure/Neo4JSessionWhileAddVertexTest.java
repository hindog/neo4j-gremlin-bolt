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

package ta.nemahuta.neo4j.structure;

import ta.nemahuta.neo4j.id.Neo4JElementIdAdapter;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.summary.ResultSummary;
import ta.nemahuta.neo4j.partition.Neo4JGraphPartition;

import java.util.Iterator;

/**
 * @author Rogelio J. Baucells
 */
@RunWith(MockitoJUnitRunner.class)
public class Neo4JSessionWhileAddVertexTest {

    @Mock
    private Neo4JGraph graph;

    @Mock
    private Transaction transaction;

    @Mock
    private Neo4JElementIdAdapter provider;

    @Mock
    private Graph.Features.VertexFeatures vertexFeatures;

    @Mock
    private Graph.Features features;

    @Mock
    private Neo4JGraphPartition partition;

    @Mock
    private Session session;

    @Mock
    private org.neo4j.driver.v1.Transaction neo4jTransaction;

    @Mock
    private StatementResult statementResult;

    @Mock
    private ResultSummary resultSummary;

    @Test
    public void givenEmptyKeyValuePairsShouldCreateVertexWithDefaultLabel() {
        // arrange
        Mockito.when(graph.tx()).thenAnswer(invocation -> transaction);
        Mockito.when(graph.getPartition()).thenAnswer(invocation -> partition);
        Mockito.when(provider.propertyName()).thenAnswer(invocation -> "whereId");
        Mockito.when(provider.generate()).thenAnswer(invocation -> 1L);
        Mockito.when(provider.convert(Mockito.any())).thenAnswer(invocation -> 1L);
        try (Neo4JSession session = new Neo4JSession(graph, this.session, provider, provider)) {
            // act
            Vertex vertex = session.addVertex();
            // assert
            Assert.assertNotNull("Failed to create vertex", vertex);
            Assert.assertEquals("Failed to assign vertex label", Vertex.DEFAULT_LABEL, vertex.label());
        }
    }

    @Test
    public void givenEmptyKeyValuePairsShouldCreateVertexWithId() {
        // arrange
        Mockito.when(graph.tx()).thenAnswer(invocation -> transaction);
        Mockito.when(graph.getPartition()).thenAnswer(invocation -> partition);
        Mockito.when(provider.propertyName()).thenAnswer(invocation -> "whereId");
        Mockito.when(provider.generate()).thenAnswer(invocation -> 1L);
        Mockito.when(provider.convert(Mockito.anyInt())).thenAnswer(invocation -> 1L);
        try (Neo4JSession session = new Neo4JSession(graph, this.session, provider, provider)) {
            // act
            Vertex vertex = session.addVertex();
            // assert
            Assert.assertNotNull("Failed to create vertex", vertex);
            Assert.assertEquals("Failed to assign vertex whereId", 1L, vertex.id());
        }
    }

    @Test
    public void givenLabelShouldCreateVertex() {
        // arrange
        Mockito.when(graph.tx()).thenAnswer(invocation -> transaction);
        Mockito.when(graph.getPartition()).thenAnswer(invocation -> partition);
        Mockito.when(provider.propertyName()).thenAnswer(invocation -> "whereId");
        Mockito.when(provider.generate()).thenAnswer(invocation -> 1L);
        Mockito.when(provider.convert(Mockito.any())).thenAnswer(invocation -> 1L);
        try (Neo4JSession session = new Neo4JSession(graph, this.session, provider, provider)) {
            // act
            Vertex vertex = session.addVertex(T.label, "label1");
            // assert
            Assert.assertNotNull("Failed to create vertex", vertex);
            Assert.assertEquals("Failed to assign vertex label", "label1", vertex.label());
        }
    }

    @Test
    public void givenLabelsShouldCreateVertex() {
        // arrange
        Mockito.when(graph.tx()).thenAnswer(invocation -> transaction);
        Mockito.when(graph.getPartition()).thenAnswer(invocation -> partition);
        Mockito.when(provider.propertyName()).thenAnswer(invocation -> "whereId");
        Mockito.when(provider.generate()).thenAnswer(invocation -> 1L);
        Mockito.when(provider.convert(Mockito.any())).thenAnswer(invocation -> 1L);
        try (Neo4JSession session = new Neo4JSession(graph, this.session, provider, provider)) {
            // act
            Neo4JVertex vertex = session.addVertex(T.label, "label1::label2::label3");
            // assert
            Assert.assertNotNull("Failed to create vertex", vertex);
            Assert.assertEquals("Failed to assign vertex label", "label1::label2::label3", vertex.label());
            Assert.assertArrayEquals("Failed to assign vertex orLabelsAnd", new String[]{"label1", "label2", "label3"}, vertex.labels());
        }
    }

    @Test
    public void givenKeyValuePairsShouldCreateVertexWithProperties() {
        // arrange
        Mockito.when(vertexFeatures.getCardinality(Mockito.anyString())).thenAnswer(invocation -> VertexProperty.Cardinality.single);
        Mockito.when(features.vertex()).thenAnswer(invocation -> vertexFeatures);
        Mockito.when(graph.tx()).thenAnswer(invocation -> transaction);
        Mockito.when(graph.getPartition()).thenAnswer(invocation -> partition);
        Mockito.when(graph.features()).thenAnswer(invocation -> features);
        Mockito.when(provider.propertyName()).thenAnswer(invocation -> "whereId");
        Mockito.when(provider.generate()).thenAnswer(invocation -> 1L);
        Mockito.when(provider.convert(Mockito.any())).thenAnswer(invocation -> 1L);
        try (Neo4JSession session = new Neo4JSession(graph, this.session, provider, provider)) {
            // act
            Neo4JVertex vertex = session.addVertex("k1", "v1", "k2", 2L, "k3", true);
            // assert
            Assert.assertNotNull("Failed to create vertex", vertex);
            Assert.assertNotNull("Failed to assign vertex property", vertex.property("k1"));
            Assert.assertEquals("Failed to assign vertex label", vertex.property("k1").value(), "v1");
            Assert.assertNotNull("Failed to assign vertex property", vertex.property("k2"));
            Assert.assertEquals("Failed to assign vertex label", vertex.property("k2").value(), 2L);
            Assert.assertNotNull("Failed to assign vertex property", vertex.property("k3"));
            Assert.assertEquals("Failed to assign vertex label", vertex.property("k3").value(), true);
        }
    }

    @Test
    public void givenNewVertexWithIdShouldBeAvailableOnIdQueries() {
        // arrange
        Mockito.when(graph.tx()).thenAnswer(invocation -> transaction);
        Mockito.when(graph.getPartition()).thenAnswer(invocation -> partition);
        Mockito.when(provider.propertyName()).thenAnswer(invocation -> "whereId");
        Mockito.when(provider.generate()).thenAnswer(invocation -> 1L);
        Mockito.when(provider.convert(Mockito.any())).thenAnswer(invocation -> 1L);
        try (Neo4JSession session = new Neo4JSession(graph, this.session, provider, provider)) {
            // add vertex
            session.addVertex();
            // act
            Iterator<Vertex> vertices = session.vertices(new Object[]{1L});
            // assert
            Assert.assertNotNull("Failed to find vertex", vertices.hasNext());
            Vertex vertex = vertices.next();
            Assert.assertNotNull("Failed to create vertex", vertex);
            Assert.assertEquals("Failed to assign vertex label", Vertex.DEFAULT_LABEL, vertex.label());
        }
    }

    @Test
    public void givenNewVertexWithIdShouldBeAvailableOnAllIdsQueries() {
        // arrange
        Mockito.when(graph.tx()).thenAnswer(invocation -> transaction);
        Mockito.when(graph.getPartition()).thenAnswer(invocation -> partition);
        Mockito.when(session.beginTransaction()).then(invocation -> neo4jTransaction);
        Mockito.when(neo4jTransaction.run(Mockito.any(Statement.class))).then(invocation -> statementResult);
        Mockito.when(statementResult.hasNext()).then(invocation -> false);
        Mockito.when(statementResult.consume()).then(invocation -> resultSummary);
        Mockito.when(provider.propertyName()).thenAnswer(invocation -> "whereId");
        Mockito.when(provider.generate()).thenAnswer(invocation -> 1L);
        ArgumentCaptor<Long> argument = ArgumentCaptor.forClass(Long.class);
        Mockito.when(provider.convert(argument.capture())).thenAnswer(invocation -> argument.getValue());
        try (Neo4JSession session = new Neo4JSession(graph, this.session, provider, provider)) {
            // transaction
            try (org.neo4j.driver.v1.Transaction tx = session.beginTransaction()) {
                // add vertex
                session.addVertex();
                // act
                Iterator<Vertex> vertices = session.vertices(new Object[0]);
                // assert
                Assert.assertNotNull("Failed to find vertex", vertices.hasNext());
                Vertex vertex = vertices.next();
                Assert.assertNotNull("Failed to create vertex", vertex);
                Assert.assertEquals("Failed to assign vertex label", Vertex.DEFAULT_LABEL, vertex.label());
                // commit
                tx.success();
            }
        }
    }

    @Test
    public void givenNewVertexWithoutIdShouldBeAvailableOnAllIdsQueries() {
        // arrange
        Mockito.when(graph.tx()).thenAnswer(invocation -> transaction);
        Mockito.when(graph.getPartition()).thenAnswer(invocation -> partition);
        Mockito.when(session.beginTransaction()).then(invocation -> neo4jTransaction);
        Mockito.when(neo4jTransaction.run(Mockito.any(Statement.class))).then(invocation -> statementResult);
        Mockito.when(statementResult.hasNext()).then(invocation -> false);
        Mockito.when(statementResult.consume()).then(invocation -> resultSummary);
        Mockito.when(provider.propertyName()).thenAnswer(invocation -> "whereId");
        Mockito.when(provider.generate()).thenAnswer(invocation -> null);
        ArgumentCaptor<Long> argument = ArgumentCaptor.forClass(Long.class);
        Mockito.when(provider.convert(argument.capture())).thenAnswer(invocation -> argument.getValue());
        try (Neo4JSession session = new Neo4JSession(graph, this.session, provider, provider)) {
            // transaction
            try (org.neo4j.driver.v1.Transaction tx = session.beginTransaction()) {
                // add vertex
                session.addVertex();
                // act
                Iterator<Vertex> vertices = session.vertices(new Object[0]);
                // assert
                Assert.assertNotNull("Failed to find vertex", vertices.hasNext());
                Vertex vertex = vertices.next();
                Assert.assertNotNull("Failed to create vertex", vertex);
                Assert.assertEquals("Failed to assign vertex label", Vertex.DEFAULT_LABEL, vertex.label());
                // commit
                tx.success();
            }
        }
    }
}
