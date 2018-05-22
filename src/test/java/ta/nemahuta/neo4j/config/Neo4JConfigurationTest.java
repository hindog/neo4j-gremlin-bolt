package ta.nemahuta.neo4j.config;

import org.apache.commons.configuration.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;

import static org.junit.jupiter.api.Assertions.assertEquals;


@ExtendWith(MockitoExtension.class)
class Neo4JConfigurationTest {

    @Mock
    private Driver driver;

    private final Neo4JConfiguration.Neo4JConfigurationBuilder builder =
            Neo4JConfiguration.builder()
                    .additionConfiguration(c -> {
                    })
                    .authToken(AuthTokens.basic("x", "y"))
                    .graphName("hoobastank")
                    .hostname("tycho.belt")
                    .port(1234)
                    .profilingEnabled(true);

    protected Neo4JConfiguration config() {
        return builder.build();
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