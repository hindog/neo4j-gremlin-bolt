package ta.nemahuta.neo4j.testutils;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.neo4j.driver.internal.types.TypeConstructor;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;
import ta.nemahuta.neo4j.session.StatementExecutor;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static ta.nemahuta.neo4j.testutils.MockUtils.*;

public class StatementExecutorStub implements StatementExecutor {

    private final Map<String, StatementResult> statementStubs = new HashMap<>();

    @Nullable
    @Override
    public StatementResult executeStatement(@Nonnull final Statement statement) {
        final String key = getKey(statement.text(), statement.parameters().asMap());
        return Optional.ofNullable(statementStubs.get(key))
                .orElseThrow(() -> new IllegalStateException("No stub exists for statement:\n" +
                        key +
                        "\nin\n - " +
                        statementStubs.keySet().stream().map(Object::toString).collect(Collectors.joining("\n - "))));

    }

    public void stubVertexAndEdgeIdFindOutBound(@Nullable Long id) {
        stubVertexAndEdgeIdFind("MATCH (n:`graphName`)-[r:`a`|:`b`]->(m:`graphName`) " + (id != null ? "WHERE ID(n)={vertexId1} " : "") + "RETURN ID(r)",
                id != null ? ImmutableMap.of("vertexId1", 2l) : ImmutableMap.of());
    }

    public void stubVertexAndEdgeIdFindInBound(@Nullable Long id) {
        stubVertexAndEdgeIdFind("MATCH (n:`graphName`)<-[r:`a`|:`b`]-(m:`graphName`) " + (id != null ? "WHERE ID(n)={vertexId1} " : "") + "RETURN ID(r)",
                id != null ? ImmutableMap.of("vertexId1", 2l) : ImmutableMap.of());
    }


    public void stubVertexAndEdgeIdFind(final String text, final Map<String, Object> params) {
        stubStatementExecution(text, params, mockStatementResult(mockRecord(
                mockValue(Value::asObject, null, 2l)
        )));
    }

    public void stubEdgeLoad(final String text, final Map<String, Object> params, final long id, final long indId, final long outId) {
        stubStatementExecution(text, params, mockStatementResult(mockRecord(
                mockValue(Value::asRelationship, null, mockRelationship(id, "remote", ImmutableMap.of("x", "y"), indId, outId))
        )));
    }

    public void stubStatementExecution(final String text, final Map<String, Object> params, final StatementResult statementResult) {
        statementStubs.put(getKey(text, params), statementResult);
    }

    @Nonnull
    private String getKey(final String text, final Map<String, Object> params) {
        return text + " - " + params.keySet().stream().sorted().map(k -> k + "=" + params.get(k)).collect(Collectors.joining(", "));
    }

    public void stubVertexIdForLabel(final String label) {
        stubVertexIdFind("MATCH (v:`" + label + "`:`graphName`) RETURN ID(v)", ImmutableMap.of());
    }

    private void stubVertexIdFind(final String text, final ImmutableMap<String, Object> params) {
        stubStatementExecution(text, params, mockStatementResult(mockRecord(
                mockValue(Value::asObject, null, 2l)
        )));
    }

    public void stubVertexLoad(final String text, final Map<String, Object> params, long id) {
        stubStatementExecution(text, params, mockStatementResult(mockRecord(
                mockValue(Value::asNode, null, mockNode(id, ImmutableSet.of("remote"), ImmutableMap.of("x", "y")))
        )));
    }

    public void stubVertexCreate(final String text, final Map<String, Object> params, final long id) {
        stubStatementExecution(text, params, mockStatementResult(mockRecord(
                mockValue(Value::asNumber, TypeConstructor.NUMBER, id)
        )));
    }

    public void stubEdgeCreate(final String text, final Map<String, Object> params, final long id) {
        stubStatementExecution(text, params, mockStatementResult(mockRecord(
                mockValue(Value::asNumber, TypeConstructor.NUMBER, id)
        )));

    }
}
