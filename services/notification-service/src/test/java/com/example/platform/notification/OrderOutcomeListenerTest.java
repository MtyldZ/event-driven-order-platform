package com.example.platform.notification;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.never;

/** Unit test of the listener: the repository is a Mockito mock, no Spring, no Kafka. */
class OrderOutcomeListenerTest {

    NotificationRepository notifications = mock(NotificationRepository.class);
    OrderOutcomeListener listener = new OrderOutcomeListener(notifications);

    @Test
    void duplicateEventIsNotStoredAgain() {
        var event = cancelled(UUID.randomUUID(), "reason");
        when(notifications.existsByEventId(event.eventId())).thenReturn(true);

        listener.onOrderOutcome(event);

        verify(notifications, never()).save(any());
    }

    @Test
    void cancelledWithoutReasonHasACleanMessage() {
        var id = UUID.randomUUID();
        listener.onOrderOutcome(cancelled(id, null));

        ArgumentCaptor<Notification> saved = ArgumentCaptor.forClass(Notification.class);
        verify(notifications).save(saved.capture());

        assertThat(saved.getValue()).extracting("message").isEqualTo("Your order " + id + " was cancelled.");
    }

    private static OrderOutcomeListener.EventEnvelope cancelled(UUID orderId, String reason) {
        return new OrderOutcomeListener.EventEnvelope(UUID.randomUUID(), "order.cancelled", Instant.now(), orderId,
            new OrderOutcomeListener.Payload("cust-test", reason));
    }
}
