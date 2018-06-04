# neo4j-gremlin-bolt

This project allows the use of the [Apache Tinkerpop](http://tinkerpop.apache.org/) Java API with the [neo4j server](http://neo4j.com/) using the [BOLT](https://github.com/neo4j/neo4j-java-driver) protocol.

## Kudos

I forked this project from [Steelbridge Labs](https://github.com/SteelBridgeLabs/neo4j-gremlin-bolt) and alhough I have nearly re-engineered
each and every part of the code, I want to acknowledge their work. 

## Build status

![Build Status](https://travis-ci.org/Tanemahuta/neo4j-gremlin-bolt.svg?branch=develop)
![Coverage Status](https://codecov.io/gh/Tanemahuta/neo4j-gremlin-bolt/branch/develop/graph/badge.svg)
[ ![Download](https://api.bintray.com/packages/tanemahuta/neo4j/neo4j-gremlin-bolt/images/download.svg) ](https://bintray.com/tanemahuta/neo4j/neo4j-gremlin-bolt/_latestVersion)

## Requirements for building

* Java 8.
* Gradle 4.x or newer (use the wrapper, please)

## Usage
Use the following repository: 
[bintray]()https://bintray.com/tanemahuta/neo4j/neo4j-gremlin-bolt)

and add the Neo4j [Apache Tinkerpop](http://tinkerpop.apache.org/) implementation and a cache provider to your project:

### Maven

```xml
    <dependency>
        <groupId>ta.nemahuta.neo4j</groupId>
        <artifactId>neo4j-gremlin-bolt</artifactId>
        <version>{version}</version>
    </dependency>
    <dependency>
        <groupId>org.ehcache</groupId>
        <artifactId>ehcache</artifactId>
        <version>{ehCacheVersion}</version>
    </dependency>
```

### Gradle
```groovy
dependencies {
    compile 'ta.nemahuta.neo4j:neo4j-gremlin-bolt:{version}'
    runtime 'org.ehcache:ehcache:{ehCacheVersion}'
}
```

## License

neo4j-gremlin-bolt and it's modules are licensed under the [Apache License v 2.0](http://www.apache.org/licenses/LICENSE-2.0).

## Features

* [Apache Tinkerpop](http://tinkerpop.apache.org/) 3.x Online Transactional Processing Graph Systems (OLTP) support.
* [neo4j](http://neo4j.com/) implementation on top of the [BOLT](https://github.com/neo4j/neo4j-java-driver) protocol.

## Graph API

### Graph configuration
The graph configuration will be used to connect to the graph.
You can use the builder to create one:
```java
    // Create a configuration using basic authentication
    final Neo4JConfiguration config = Neo4JConfiguration.builder()
                        .graphName("partitionLabel") // this is optional
                        .hostname("localhost")
                        .port(7687)
                        .authToken(AuthTokens.basic("neo4j", "neo4j123"))
                        .cacheExpiry("30.minutes")
                        .build();
```

### Graph Factory
The graph factory is being to share a session (including the connection pool of it) and a global cache of loaded elements 
in an environment.
To obtain a graph factory just create a new one using the configuration:
```java
    Neo4JGraphFactory graphFactory = new Neo4JGraphFactory(config);
```

### Working with transactions

* Obtain a [Transaction](http://tinkerpop.apache.org/javadocs/current/core/org/apache/tinkerpop/gremlin/structure/Transaction.html) instance from current Graph.

```java
    // create graph instance
    try (Graph graph = graphFactory.get()) {
        // begin transaction
        try (Transaction transaction = graph.tx()) {
            // use Graph API to create, update and delete Vertices and Edges
            
            // commit transaction
            transaction.commit();
        }
    }
```

### Working with Vertices and Edges

#### Create a Vertex

Create a new [Vertex](http://tinkerpop.apache.org/javadocs/current/core/org/apache/tinkerpop/gremlin/structure/Vertex.html) in the current `graph` call the [Graph.addVertex()](http://tinkerpop.apache.org/javadocs/current/core/org/apache/tinkerpop/gremlin/structure/Graph.html#addVertex-java.lang.Object...-) method.

```java
  // create a vertex in current graph
  Vertex vertex = graph.addVertex();
```

Create a new [Vertex](http://tinkerpop.apache.org/javadocs/current/core/org/apache/tinkerpop/gremlin/structure/Vertex.html) in the current `graph` with property values: 

```java
  // create a vertex in current graph with property values
  Vertex vertex = graph.addVertex("name", "John", "age", 50);
```

Create a new [Vertex](http://tinkerpop.apache.org/javadocs/current/core/org/apache/tinkerpop/gremlin/structure/Vertex.html) in the current `graph` with a Label: 

```java
  // create a vertex in current graph with label
  Vertex vertex1 = graph.addVertex("Person");
  // create another vertex in current graph with label
  Vertex vertex2 = graph.addVertex(T.label, "Company");
```

## Caching
This project heavily uses caching when obtaining a graph.

Since no `GraphComputer` for gremlin is created, the vertices will normally be completely loaded, and thus the implementation 
is sped up by using a global cache for each `Neo4JGraphFactory`.

The global caches will be used in the session caches and the session caches are committed to the global cache, each time a transaction
has been committed.

Some words of warning regarding asynchronous usage:
- each transaction has to be handled in the same thread it has been created in, solely! (this is a limitation of the bolt driver)
- the graphs and the element states _should be_ thread safe per se, the connector (bolt-driver) seems not to be
- be aware that you can produce data races by executing transactions out-of-order any-time, so make sure you synchronize the operations 
accordingly  
- we are using the connector asynchronously using `ReadWriteLocks` for certain conditions, and it works smoothly

## Building the library

To compile the code and run all the unit tests:

````
./gradlew test assemble
````

To run the Tinkerpop integration tests you need a running instance of the neo4j
server. The easiest way to get one up and running is by using the official neo4j
docker image:

````
docker run -d --name neo4j -p 7687:7687 -e NEO4J_AUTH=neo4j/neo4j123 neo4j:3.2-enterprise
````

And then execute the integration tests by running the following command:

````
./gradlew integrationTest
````
