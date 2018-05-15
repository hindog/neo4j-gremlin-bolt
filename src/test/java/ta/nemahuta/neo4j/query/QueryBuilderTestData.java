package ta.nemahuta.neo4j.query;

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.function.Consumer;

public class QueryBuilderTestData<T extends StatementBuilder> {

    public final T source;
    public final String expectedText;
    public final Map<String, Object> expectedParameters;

    public QueryBuilderTestData(final T query,
                                final String expectedText) {
        this(query, expectedText, c -> {
        });
    }

    public QueryBuilderTestData(final T query,
                                final String expectedText,
                                final Consumer<ImmutableMap.Builder<String, Object>> parametersConsumer) {
        this.source = query;
        this.expectedText = expectedText;
        final ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();
        parametersConsumer.accept(mapBuilder);
        this.expectedParameters = mapBuilder.build();
    }

    @Override
    public String toString() {
        return expectedText + " using " + expectedParameters;
    }

}
