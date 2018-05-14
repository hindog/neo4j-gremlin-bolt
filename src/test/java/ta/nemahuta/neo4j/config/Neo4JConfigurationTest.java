package ta.nemahuta.neo4j.config;

import org.apache.commons.configuration.Configuration;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import ta.nemahuta.neo4j.id.Neo4JElementIdAdapter;
import ta.nemahuta.neo4j.id.Neo4JNativeElementIdAdapter;

import static org.junit.jupiter.api.Assertions.assertEquals;


@ExtendWith(MockitoExtension.class)
class Neo4JConfigurationTest {

    @Mock
    private Driver driver;
    @Mock
    private Neo4JElementIdAdapter<?> edgeAdapter, vertexAdapter;

    private final Neo4JConfiguration.Neo4JConfigurationBuilder builder =
            Neo4JConfiguration.builder()
                    .additionConfiguration(c -> {
                    })
                    .authToken(AuthTokens.basic("x", "y"))
                    .edgeIdAdapterFactory(d -> edgeAdapter)
                    .vertexIdAdapterFactory(d -> vertexAdapter)
                    .graphName("hoobastank")
                    .hostname("tycho.belt")
                    .port(1234)
                    .profilingEnabled(true);

    protected Neo4JConfiguration config() {
        return builder.build();
    }

    @Nested
    class CreateVertexIdAdapter {
        @Test
        void createsAdapterFromConfig() {
            // expect: 'the adapter from the factory'
            assertEquals(config().createVertexIdAdapter(driver), vertexAdapter);
        }

        @Test
        void createsDefaultAdapter() {
            // when: 'removing the adapter factory'
            final Neo4JConfiguration config = builder.vertexIdAdapterFactory(null).build();
            // then: 'the adapter from the
            assertEquals(config.createVertexIdAdapter(driver).getClass(), Neo4JNativeElementIdAdapter.class);
        }
    }

    @Nested
    class CreateEdgeIdAdapter {
        @Test
        void createsAdapterFromConfig() {
            // expect: 'the adapter from the factory'
            assertEquals(config().createEdgeIdAdapter(driver), edgeAdapter);
        }

        @Test
        void createsDefaultAdapter() {
            // when: 'removing the adapter factory'
            final Neo4JConfiguration config = builder.edgeIdAdapterFactory(null).build();
            // then: 'the adapter from the
            assertEquals(config.createEdgeIdAdapter(driver).getClass(), Neo4JNativeElementIdAdapter.class);
        }
    }

    @Test
    void apacheConfigurationConversion() {
        // setup: 'a neo4j configuration'
        final Neo4JConfiguration neo4JConfig = config();
        // when: 'converting to the apache configuration'
        final Configuration apacheConfig = neo4JConfig.toApacheConfiguration();
        // and: 'converting back'
        final Neo4JConfiguration actualConfig = Neo4JConfiguration.fromApacheConfiguration(apacheConfig);
        // then: 'the configuration equals the one we have built'
        assertEquals(actualConfig, neo4JConfig);
    }
}