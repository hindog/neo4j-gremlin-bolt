package ta.nemahuta.neo4j.session;

import lombok.extern.slf4j.Slf4j;
import org.apache.tinkerpop.gremlin.structure.util.AbstractThreadedTransaction;
import org.apache.tinkerpop.gremlin.structure.util.TransactionException;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ta.nemahuta.neo4j.structure.Neo4JGraph;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;

/**
 * Gremlin based wrapper for wrapping the transactions of a {@link Session}.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
@Slf4j
public class Neo4JTransaction extends AbstractThreadedTransaction implements StatementExecutor {

    private final Session session;
    private Transaction wrapped;

    private static final Logger statementLogger = LoggerFactory.getLogger(Neo4JTransaction.class.getPackage().getName() + ".Statement");
    private boolean modification = false;

    public Neo4JTransaction(@Nonnull final Neo4JGraph g,
                            @Nonnull final Session session) {
        super(g);
        this.session = session;
    }

    /**
     * Marks the transaction to modify the underlying graph database.
     */
    public void markModifying() {
        this.modification = true;
    }

    @Override
    protected synchronized void doOpen() {
        this.wrapped = Optional.ofNullable(this.wrapped).orElseGet(session::beginTransaction);
    }

    @Override
    protected void doCommit() throws TransactionException {
        log.debug("Committing all entities in session scope of transaction {} in session {}", transactionHashCode(), session.hashCode());

        log.debug("Committing all entities in session scope of transaction {} in session {}", transactionHashCode(), session.hashCode());
        Optional.ofNullable(wrapped)
                .orElseThrow(org.apache.tinkerpop.gremlin.structure.Transaction.Exceptions::transactionMustBeOpenToReadWrite)
                .success();
    }

    @Override
    protected void doReadWrite() {
        if (!isOpen()) {
            open();
            log.debug("Opened transaction {} for session {}", transactionHashCode(), session.hashCode());
        }
    }

    @Override
    protected void doRollback() throws TransactionException {
        log.debug("Rolling back all entities in sessions scope of transaction {} in session {}", transactionHashCode(), session.hashCode());

        Optional.ofNullable(wrapped)
                .orElseThrow(org.apache.tinkerpop.gremlin.structure.Transaction.Exceptions::transactionMustBeOpenToReadWrite)
                .failure();
    }

    @Override
    public void doClose() {
        if (isOpen()) {
            log.debug("Closing transaction {} for session {}", transactionHashCode(), session.hashCode());
            super.doClose();
            Optional.ofNullable(wrapped)
                    .orElseThrow(org.apache.tinkerpop.gremlin.structure.Transaction.Exceptions::transactionMustBeOpenToReadWrite)
                    .close();
            this.wrapped = null;
        }
    }

    @Override
    public boolean isOpen() {
        return wrapped != null;
    }

    @Nullable
    @Override
    public StatementResult executeStatement(@Nonnull final Statement statement) {
        readWrite();
        statementLogger.debug("Execution in transaction {} for session {}: '{}' with {}", transactionHashCode(), session.hashCode(), statement.text(), statement.parameters());
        return wrapped.run(statement);
    }

    private Integer transactionHashCode() {
        return Optional.ofNullable(wrapped).map(Object::hashCode).orElse(null);
    }

}
