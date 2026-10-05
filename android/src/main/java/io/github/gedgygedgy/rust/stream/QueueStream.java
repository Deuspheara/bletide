package io.github.gedgygedgy.rust.stream;

import io.github.gedgygedgy.rust.task.PollResult;
import io.github.gedgygedgy.rust.task.Waker;
import io.github.gedgygedgy.rust.future.FutureException;

import java.util.LinkedList;
import java.util.Queue;

/**
 * Simple implementation of {@link Stream} which can be woken with items.
 * In general, methods which create a {@link QueueStream} should return it as
 * a {@link Stream} to keep calling code from waking it.
 */
public class QueueStream<T> implements Stream<T> {
    private Waker waker = null;
    private final Queue<T> result = new LinkedList<>();
    private boolean finished = false;
    private Throwable failure = null;
    private final int capacity;
    private final Object lock = new Object();

    /**
     * Creates a new {@link QueueStream} object.
     */
    public QueueStream() { this(Integer.MAX_VALUE); }

    /** A bounded stream terminates and discards its backlog on overflow. */
    public QueueStream(int capacity) {
        if (capacity <= 0) throw new IllegalArgumentException("Invalid stream capacity");
        this.capacity = capacity;
    }

    public boolean isFinished() {
        synchronized (this.lock) { return this.finished; }
    }

    /** Deterministic disposal, including queued payloads and the native waker. */
    public void close() {
        Waker pending;
        synchronized (this.lock) {
            this.finished = true;
            this.result.clear();
            pending = this.waker;
            this.waker = null;
        }
        if (pending != null) pending.wake();
    }

    @Override
    public PollResult<StreamPoll<T>> pollNext(Waker waker) {
        PollResult<StreamPoll<T>> result = null;
        Waker oldWaker = null;
        synchronized (this.lock) {
            if (!this.result.isEmpty()) {
                T item = this.result.remove();
                result = () -> () -> item;
            } else if (this.finished) {
                Throwable failure = this.failure;
                this.failure = null;
                result = failure == null ? () -> null : () -> { throw new FutureException(failure); };
            } else {
                oldWaker = this.waker;
                this.waker = waker;
            }
        }
        if (oldWaker != null) {
            oldWaker.close();
        }
        if (result != null) {
            waker.close();
        }
        return result;
    }

    private void doEvent(Runnable r) {
        Waker waker = null;
        synchronized (this.lock) {
            if (this.finished) return;
            r.run();
            waker = this.waker;
            this.waker = null;
        }
        if (waker != null) {
            waker.wake();
        }
    }

    /**
     * Adds a new item to the queue of items to be returned by
     * {@link pollNext}. This can be anything, including {@code null}.
     *
     * @param item Item to add to the queue.
     */
    public void add(T item) {
        this.doEvent(() -> {
            synchronized (this.lock) {
                if (this.result.size() >= this.capacity) {
                    this.result.clear();
                    this.finished = true;
                    this.failure = new IllegalStateException("Notification queue overflow (capacity: " + this.capacity + ")");
                } else {
                    this.result.add(item);
                }
            }
        });
    }

    /**
     * Marks the queue as finished. After the queue is finished, no new items
     * can be added. Once all existing items have been drained from the queue,
     * the {@link PollResult} returned from {@link pollNext} will return
     * {@code null} from its own {@link PollResult#get}.
     */
    public void finish() {
        this.doEvent(() -> this.finished = true);
    }
}
