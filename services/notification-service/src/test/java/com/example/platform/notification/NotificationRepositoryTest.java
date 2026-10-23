package com.example.platform.notification;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataIntegrityViolationException;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

// JPA slice: only JPA + Flyway start, and each test is rolled back. saveAndFlush sends the INSERT now,
// so the database constraint is what gets tested (not Hibernate's queue of pending inserts).
@DataJpaTest
@Import(TestcontainersConfiguration.class)
class NotificationRepositoryTest {

    @Autowired NotificationRepository notifications;

    @Test
    void sameEventIdCannotBeStoredTwice() {
        var eventId = UUID.randomUUID();
        var first = new Notification(eventId, UUID.randomUUID(), "cust", "type", "message");
        var duplicate = new Notification(eventId, UUID.randomUUID(), "cust", "type", "message");

        notifications.saveAndFlush(first);
        assertThatThrownBy(() -> notifications.saveAndFlush(duplicate)).isInstanceOf(DataIntegrityViolationException.class);
    }
}
