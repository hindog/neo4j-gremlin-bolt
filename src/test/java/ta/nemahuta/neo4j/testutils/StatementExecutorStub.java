package ta.nemahuta.neo4j.testutils;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.javatuples.Pair;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;
import ta.nemahuta.neo4j.id.Neo4JElementId;
import ta.nemahuta.neo4j.session.StatementExecutor;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static ta.nemahuta.neo4j.testutils.MockUtils.*;

public class StatementExecutorStub implements StatementExecutor {

    private final Map<Pair<String, Map<String, Object>>, StatementResult> statementStubs = new HashMap<>();

    @Nullable
    @Override
    public StatementResult executeStatement(@Nonnull final Statement statement) {
        final Pair<String, Object> key = new Pair<>(statement.text(), statement.parameters().asObject());
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

    public void stubEdgeLoad(final String text, final Map<String, Object> params, final Neo4JElementId<?> indId, final Neo4JElementId<?> outId) {
        stubStatementExecution(text, params, mockStatementResult(mockRecord(
                mockValue(Value::asObject, null, outId.getId()),
                mockValue(Value::asRelationship, null, mockRelationship(2l, "remote", ImmutableMap.of("x", "y"))),
                mockValue(Value::asObject, null, indId.getId())
        )));
    }

    public void stubStatementExecution(final String text, final Map<String, Object> params, final StatementResult statementResult) {
        statementStubs.put(new Pair<>(text, params), statementResult);
    }

    public void stubVertexIdForLabel(final String label) {
        stubVertexIdFind("MATCH (v:`" + label + "`:`graphName`) RETURN ID(v)", ImmutableMap.of());
    }

    private void stubVertexIdFind(final String text, final ImmutableMap<String, Object> params) {
        stubStatementExecution(text, params, mockStatementResult(mockRecord(
                mockValue(Value::asObject, null, 2l)
        )));
    }

    public void stubVertexLoad(final String text, final Map<String, Object> params) {
        stubStatementExecution(text, params, mockStatementResult(mockRecord(
                mockValue(Value::asNode, null, mockNode(2l, ImmutableSet.of("remote"), ImmutableMap.of("x", "y")))
        )));
    }

}
