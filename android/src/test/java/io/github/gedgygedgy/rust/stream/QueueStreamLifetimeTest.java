package io.github.gedgygedgy.rust.stream;

import io.github.gedgygedgy.rust.task.Waker;
import io.github.gedgygedgy.rust.future.FutureException;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class QueueStreamLifetimeTest {
    @Test public void overflowEndsStreamAndReleasesBacklogAndPendingWaker() {
        QueueStream<byte[]> stream = new QueueStream<>(2);
        Waker pending = mock(Waker.class);
        assertNull(stream.pollNext(pending));
        stream.add(new byte[] {1});
        verify(pending).wake();
        stream.add(new byte[] {2});
        stream.add(new byte[] {3});
        assertTrue(stream.isFinished());
        stream.add(new byte[] {4});
        Waker poll = mock(Waker.class);
        var failure = assertThrows(FutureException.class, () -> stream.pollNext(poll).get());
        assertEquals("Notification queue overflow (capacity: 2)", failure.getCause().getMessage());
        assertNull(stream.pollNext(mock(Waker.class)).get());
        verify(poll).close();
        verifyNoMoreInteractions(pending);
    }
    @Test public void overflowCauseSurvivesCloseAndIsReleasedAfterOnePoll() {
        for (int capacity : new int[]{1,2,1024}) {
            QueueStream<Integer> stream = new QueueStream<>(capacity);
            for (int value=0; value<=capacity; value++) stream.add(value);
            stream.close(); stream.close(); stream.finish(); stream.add(42);
            Waker poll=mock(Waker.class);
            var result=stream.pollNext(poll);
            stream.close();
            var failure=assertThrows(FutureException.class, result::get);
            assertEquals("Notification queue overflow (capacity: " + capacity + ")",failure.getCause().getMessage());
            verify(poll).close();
            assertNull(stream.pollNext(mock(Waker.class)).get());
            assertNull(stream.pollNext(mock(Waker.class)).get());
        }
    }
    @Test public void closingBetweenPollAndGetPreservesTheAlreadyPolledItem() {
        QueueStream<Integer> stream = new QueueStream<>(2);
        stream.add(1);
        stream.add(2);
        Waker poll = mock(Waker.class);
        var item = stream.pollNext(poll);
        stream.close();
        assertEquals(Integer.valueOf(1), item.get().get());
        assertNull(stream.pollNext(mock(Waker.class)).get());
        assertTrue(stream.isFinished());
    }
    @Test public void hundredCloseCyclesReleaseExactlyOnePendingWaker() {
        for (int cycle = 0; cycle < 100; cycle++) {
            QueueStream<Integer> stream = new QueueStream<>(2);
            Waker pending = mock(Waker.class);
            assertNull(stream.pollNext(pending));
            stream.close();
            stream.close();
            stream.finish();
            stream.add(7);
            verify(pending).wake();
            verifyNoMoreInteractions(pending);
            assertNull(stream.pollNext(mock(Waker.class)).get());
        }
    }
    @Test public void ordinaryFinishPreservesFifoAndThenEnds() {
        QueueStream<Integer> stream = new QueueStream<>(2);
        stream.add(1);
        stream.add(2);
        stream.finish();
        assertEquals(Integer.valueOf(1), stream.pollNext(mock(Waker.class)).get().get());
        assertEquals(Integer.valueOf(2), stream.pollNext(mock(Waker.class)).get().get());
        assertNull(stream.pollNext(mock(Waker.class)).get());
    }
}
