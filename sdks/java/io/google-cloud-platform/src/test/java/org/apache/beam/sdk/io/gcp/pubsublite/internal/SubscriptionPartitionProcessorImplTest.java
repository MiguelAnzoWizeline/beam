/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.gcp.pubsublite.internal;

import static com.google.cloud.pubsublite.internal.testing.UnitTestExamples.example;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.api.core.ApiFutures;
import com.google.api.core.SettableApiFuture;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode.Code;
import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.internal.CheckedApiException;
import com.google.cloud.pubsublite.internal.testing.FakeApiService;
import com.google.cloud.pubsublite.proto.Cursor;
import com.google.cloud.pubsublite.proto.SequencedMessage;
import com.google.protobuf.util.Timestamps;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Spy;

@RunWith(JUnit4.class)
@SuppressWarnings("initialization.fields.uninitialized")
public class SubscriptionPartitionProcessorImplTest {
  private static final SubscriptionPartition PARTITION =
      SubscriptionPartition.of(example(SubscriptionPath.class), example(Partition.class));

  @Spy RestrictionTracker<OffsetByteRange, OffsetByteProgress> tracker;
  @Mock OutputReceiver<SequencedMessage> receiver;
  @Mock Supplier<MemoryBufferedSubscriber> subscriberFactory;

  @Rule public Timeout globalTimeout = Timeout.seconds(30);

  abstract static class FakeSubscriber extends FakeApiService implements MemoryBufferedSubscriber {}

  @Spy FakeSubscriber subscriber;

  private static SequencedMessage messageWithOffset(long offset) {
    return SequencedMessage.newBuilder()
        .setCursor(Cursor.newBuilder().setOffset(offset))
        .setPublishTime(Timestamps.fromMillis(10000 + offset))
        .setSizeBytes(1024)
        .build();
  }

  private OffsetByteRange initialRange() {
    return OffsetByteRange.of(new OffsetRange(example(Offset.class).value(), Long.MAX_VALUE));
  }

  @Before
  public void setUp() {
    initMocks(this);
    when(subscriberFactory.get()).thenReturn(subscriber);
    when(tracker.currentRestriction()).thenReturn(initialRange());
    doReturn(true).when(subscriber).isRunning();
    doReturn(example(Offset.class)).when(subscriber).fetchOffset();
    doReturn(SettableApiFuture.create()).when(subscriber).onData();
  }

  private SubscriptionPartitionProcessor newProcessor() {
    return new SubscriptionPartitionProcessorImpl(PARTITION, tracker, receiver, subscriberFactory);
  }

  @Test
  public void lifecycle() {
    SubscriptionPartitionProcessor processor = newProcessor();
    assertEquals(ProcessContinuation.resume(), processor.runFor(Duration.millis(10)));
    InOrder order = inOrder(subscriberFactory, subscriber);
    order.verify(subscriberFactory).get();
    order.verify(subscriber).fetchOffset();
    order.verify(subscriber).rebuffer();
  }

  @Test
  public void lifecycleOffsetMismatch() {
    MemoryBufferedSubscriber badSubscriber = spy(FakeSubscriber.class);
    doReturn(Offset.of(example(Offset.class).value() + 1)).when(badSubscriber).fetchOffset();
    doThrow(new RuntimeException("Ignored")).when(badSubscriber).awaitTerminated();
    doReturn(badSubscriber, subscriber).when(subscriberFactory).get();
    SubscriptionPartitionProcessor processor = newProcessor();
    assertEquals(ProcessContinuation.resume(), processor.runFor(Duration.millis(10)));
    InOrder order = inOrder(subscriberFactory, badSubscriber, subscriber);
    order.verify(subscriberFactory).get();
    order.verify(badSubscriber).fetchOffset();
    order.verify(badSubscriber).stopAsync();
    order.verify(badSubscriber).awaitTerminated();
    order.verify(subscriberFactory).get();
    order.verify(subscriber).fetchOffset();
    order.verify(subscriber).rebuffer();
  }

  @Test
  public void lifecycleRebufferThrows() throws Exception {
    doThrow(new CheckedApiException(Code.OUT_OF_RANGE).underlying).when(subscriber).rebuffer();
    assertThrows(ApiException.class, this::newProcessor);
  }

  @Test
  public void subscriberFailureReturnsResume() throws Exception {
    SubscriptionPartitionProcessor processor = newProcessor();
    doReturn(ApiFutures.immediateFuture(null)).when(subscriber).onData();
    doReturn(false).when(subscriber).isRunning();
    assertEquals(ProcessContinuation.resume(), processor.runFor(Duration.standardHours(1)));
  }

  @Test
  public void timeoutReturnsResume() {
    SubscriptionPartitionProcessor processor = newProcessor();
    assertEquals(ProcessContinuation.resume(), processor.runFor(Duration.millis(10)));
    assertFalse(processor.lastClaimed().isPresent());
  }

  @Test
  public void failedClaimCausesStop() {
    SubscriptionPartitionProcessor processor = newProcessor();

    when(tracker.tryClaim(any())).thenReturn(false);
    doReturn(ApiFutures.immediateFuture(null)).when(subscriber).onData();
    doReturn(Optional.of(messageWithOffset(1))).when(subscriber).peek();

    assertEquals(ProcessContinuation.stop(), processor.runFor(Duration.standardHours(10)));

    verify(tracker, times(1)).tryClaim(any());
    verify(subscriber, times(0)).pop();
    assertFalse(processor.lastClaimed().isPresent());
  }

  @Test
  public void successfulClaimThenTimeout() {
    doReturn(true).when(tracker).tryClaim(any());
    doReturn(ApiFutures.immediateFuture(null), SettableApiFuture.create())
        .when(subscriber)
        .onData();

    SequencedMessage message1 = messageWithOffset(1);
    SequencedMessage message3 = messageWithOffset(3);
    doReturn(Optional.of(message1), Optional.of(message3), Optional.empty())
        .when(subscriber)
        .peek();

    SubscriptionPartitionProcessor processor = newProcessor();
    assertEquals(ProcessContinuation.resume(), processor.runFor(Duration.standardSeconds(3)));

    InOrder order = inOrder(tracker, receiver);
    order.verify(tracker).tryClaim(OffsetByteProgress.of(Offset.of(1), message1.getSizeBytes()));
    order
        .verify(receiver)
        .outputWithTimestamp(message1, new Instant(Timestamps.toMillis(message1.getPublishTime())));
    order.verify(tracker).tryClaim(OffsetByteProgress.of(Offset.of(3), message3.getSizeBytes()));
    order
        .verify(receiver)
        .outputWithTimestamp(message3, new Instant(Timestamps.toMillis(message3.getPublishTime())));
    assertEquals(processor.lastClaimed().get(), Offset.of(3));
  }
}
