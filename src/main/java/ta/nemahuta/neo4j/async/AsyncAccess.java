package ta.nemahuta.neo4j.async;

import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.ToString;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Wrapper for asynchronous access to a stored value.
 *
 * @param <T> the type of the wrapped value
 * @author Christian Heike (christian.heike@icloud.com)
 */
@EqualsAndHashCode(of = "wrapped")
@ToString(of = "wrapped")
public class AsyncAccess<T> {

    /**
     * the wrapped value
     */
    private T wrapped;

    /**
     * the lock provider
     */
    private final ReadWriteLock lockProvider = new ReentrantReadWriteLock();

    /**
     * Create a new wrapper for an initial value.
     *
     * @param wrapped the initial value
     */
    public AsyncAccess(@Nullable final T wrapped) {
        this.wrapped = wrapped;
    }

    /**
     * Update the value using a write lock.
     *
     * @param function the function for the update serving the current value and expecting the new value
     * @return the new value
     */
    @Nullable
    public T update(@Nonnull @NonNull final Function<T, T> function) {
        final Lock lock = lockProvider.writeLock();
        lock.lock();
        try {
            this.wrapped = function.apply(this.wrapped);
            return this.wrapped;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Get the value using a read lock.
     *
     * @param function the function which retrieves a value from the currently set one
     * @param <R>      the type of the result
     * @return the result
     */
    @Nullable
    public <R> R get(@Nonnull @NonNull final Function<? super T, R> function) {
        final Lock lock = lockProvider.readLock();
        lock.lock();
        try {
            return function.apply(wrapped);
        } finally {
            lock.unlock();
        }
    }

    /**
     * @param <E> the type of the thrown exception
     * @throws E an exception from the function
     * @see #get(Function)
     */
    @Nullable
    public <E extends Throwable, R> R getThrows(@Nonnull @NonNull final ExceptionFunction<E, ? super T, R> function) throws E {
        final Lock lock = lockProvider.readLock();
        lock.lock();
        try {
            return function.apply(wrapped);
        } finally {
            lock.unlock();
        }
    }

    /**
     * @param consumer the consumer
     * @see #get(Function) using a consumer
     */
    public void consume(@Nonnull @NonNull final Consumer<? super T> consumer) {
        get(toFunction(consumer));
    }

    @Nonnull
    private static <T> Function<T, Void> toFunction(@Nonnull @NonNull final Consumer<T> consumer) {
        return r -> {
            consumer.accept(r);
            return null;
        };
    }

    /**
     * Functional interface which is able to throw checked exceptions.
     *
     * @param <E> the exception type
     * @param <T> the type of the input
     * @param <R> the result type
     */
    @FunctionalInterface
    public interface ExceptionFunction<E extends Throwable, T, R> {
        /**
         * Performs this handler on the given argument.
         *
         * @param t the input argument
         * @return the result from the function
         */
        @Nullable
        R apply(@Nullable T t) throws E;
    }

}
