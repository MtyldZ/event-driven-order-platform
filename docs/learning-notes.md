# Learning notes

What this project teaches, phase by phase: every new concept as a "How it works" note (what it is, what you observe, how it works, why we chose it, a common pitfall), the real results we observed, and three self-check questions per phase with answers.

The experiments behind Phase 6 are in [experiments.md](experiments.md); the decisions in [decisions/](decisions/).

## Phase 0: Repo and local environment

Goal: one command (`docker compose up -d`) starts Kafka, Postgres and Redis locally; verify Kafka by hand with its CLI tools.

### 🔎 HOW IT WORKS: Docker Compose services + health checks
- What it is:      A YAML file describing several containers that run together on one private network.
- What it does:    `docker compose up -d` starts Kafka, Postgres and Redis; `docker compose ps` shows each as `healthy`.
- How it does it:  Each service becomes a container. Containers reach each other by service name (`kafka`, `postgres`) through Compose's built-in DNS. A `healthcheck` is a command Docker runs repeatedly inside the container (`pg_isready`, `redis-cli ping`, `kafka-broker-api-versions.sh`); exit code 0 = healthy.
- Why we use it:   Reproducible environment in one file, same on every machine and in CI. Alternative: installing Kafka/Postgres/Redis with Homebrew (version drift, harder cleanup).
- Common pitfall:  "Started" is not "ready". Kafka takes ~10-20s after the container starts. Later, app services will use `depends_on: condition: service_healthy` so they wait.

### 🔎 HOW IT WORKS: Broker and KRaft
- What it is:      A broker is a Kafka server process that stores messages on disk and serves them to clients. KRaft is Kafka's built-in way of storing cluster metadata (which topics exist, who leads which partition).
- What it does:    Our single container is both broker (handles data) and controller (handles metadata): `KAFKA_PROCESS_ROLES: broker,controller`.
- How it does it:  Controllers keep metadata in an internal Raft-replicated log. Before KRaft, a separate ZooKeeper cluster did this job. Kafka 4.0 removed ZooKeeper entirely, so KRaft is the only option now.
- Why we use it:   One container instead of two, and it is the only supported mode. In production you would run 3+ brokers and 3 controllers for fault tolerance.
- Common pitfall:  Listeners. A client first connects to any address, then the broker replies "talk to me at <advertised address>". Containers must get `kafka:29092` (INTERNAL), your Mac must get `localhost:9092` (EXTERNAL). Wrong advertised listener = connects then hangs/fails.

### 🔎 HOW IT WORKS: Topic and partition
- What it is:      A topic is a named stream of messages (like a table name). A partition is one append-only log file that makes up part of a topic.
- What it does:    `demo` has 3 partitions. Our 6 messages landed in partitions 0 and 2; partition 1 got none.
- How it does it:  The producer hashes the message key: `partition = hash(key) % 3`. Same key → same partition, always. Inside one partition, order is guaranteed; across partitions, it is not.
- Why we use it:   Partitions are the unit of parallelism (one consumer per partition per group) and keying by `orderId` keeps all events of one order in order. That is exactly why our events use `key = orderId`.
- Common pitfall:  Adding partitions later changes `hash % N` so existing keys can move to a new partition, breaking per-key ordering for in-flight data. Pick a partition count up front.

Observed:
```
Partition:0  Offset:0  order-B  created
Partition:0  Offset:2  order-B  cancelled     <- same key, same partition, in order
Partition:2  Offset:0  order-A  created
Partition:2  Offset:1  order-A  confirmed
```

### 🔎 HOW IT WORKS: Offset
- What it is:      A message's position number inside a partition (0, 1, 2, ...).
- What it does:    Each partition counts independently; partition 0 and partition 2 both have an offset 0.
- How it does it:  The broker appends to the end of the log and assigns the next number. Messages are not deleted when read; they stay until retention (time/size) removes them.
- Why we use it:   Because reading does not delete, many independent readers can read the same data, and a reader can rewind. This is the big difference from a classic queue (RabbitMQ-style), where a consumed message is gone.
- Common pitfall:  Thinking Kafka tracks "which message was processed". It only stores one number per group per partition: "next offset to read". Everything before it counts as done.

### 🔎 HOW IT WORKS: Consumer group and committed offsets
- What it is:      A name (`group.id`) shared by consumers that split a topic's work between them.
- What it does:    `g1` read all 6 messages, then on a second run read 0 (it remembered). A new group `g2` read all 6 again.
- How it does it:  Each group commits its position per partition to the internal topic `__consumer_offsets`. `LAG = LOG-END-OFFSET - CURRENT-OFFSET` = messages not yet processed. After producing `order-A:shipped`, g1 showed lag 1 on partition 2.
- Why we use it:   Different services = different groups, so inventory-service and a future analytics service each get every event. Several instances of the same service = same group, so they share partitions (load balancing). Like many `@RestController` instances behind a load balancer, but the split is by partition.
- Common pitfall:  More consumers than partitions → the extras sit idle. 3 partitions = max 3 active consumers per group.

Observed:
```
GROUP  TOPIC  PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
g1     demo   0          4               4               0
g1     demo   1          0               0               0
g1     demo   2          2               2               0
```

### 🔎 HOW IT WORKS: Database per service
- What it is:      Each service owns a private database; no other service reads or writes its tables.
- What it does:    One Postgres container holds `orders_db`, `inventory_db`, `notifications_db`, created by `infra/postgres/init.sql` on first start.
- How it does it:  The official Postgres image runs any `*.sql` in `/docker-entrypoint-initdb.d/` once, only when the data volume is empty.
- Why we use it:   Services can change their schema and deploy independently. If order-service could `SELECT` from inventory tables, any inventory schema change could break orders: you would have a distributed monolith. Data is shared only through APIs and events. Trade-off: no cross-service JOINs or transactions, which is why we need the saga later. One container (not three) is a local-dev shortcut; we also use one DB user for all three. Separate users per database would enforce the rule technically; skipped for simplicity.
- Common pitfall:  `init.sql` does not re-run. Change it later → `docker compose down -v` (deletes the volume and all data) to re-init.

### Notes from running it
- Kafka 4.x CLI: `--property` is deprecated; use `--reader-property` (producer) and `--formatter-property` (consumer).
- Postgres 18 images changed the data path: mount the volume at `/var/lib/postgresql`, not `/var/lib/postgresql/data`.
- Pinned images (checked on Docker Hub, 2026-10-12): `apache/kafka:4.3.1`, `postgres:18.6`, `redis:8.10.2`.

### Check your understanding
1. All events of one order use `orderId` as the key. What goes wrong if events are sent **without** a key, e.g. `order.created` and `order.cancelled` for the same order?
2. inventory-service runs **5 instances** in one consumer group and `order.created` has 3 partitions. What do the 5 instances do? What changes when a second service subscribes with its own group?
3. Kafka only stores "next offset to read" per group and partition. A consumer processes a message but crashes **before committing** the offset. What happens on restart, and why does it matter?

<details>
<summary>Answers</summary>

- **Instance vs group.** 5 instances of inventory-service = 5 consumers in ONE group. With 3 partitions, 3 instances each own one partition, 2 sit idle as standby (they take over on a rebalance if one dies). A *different* service = a *different* group, which gets its own full copy of every message and its own offsets. Inside a group work is shared; between groups everything is copied.
- **Crash before commit = duplicate, not loss.** On restart the consumer resumes from the last *committed* offset, so an already-processed-but-uncommitted message is processed again. That is at-least-once delivery, and why Phase 2 needs an idempotent consumer.
- **Offset is a position inside one partition**, like a line number in a log file. It knows nothing about orders. Per-order ordering = key puts all of an order's events into one partition + offsets order them within it.
- Without a key, messages *may* land in different partitions (not guaranteed either way), so a consumer could see `order.cancelled` before `order.created`.

</details>


## Phase 1: Order service (write side)

Goal: `POST /orders` stores the order as `PENDING` in `orders_db` and publishes `order.created`. Learn JPA mappings, transactions and the N+1 problem along the way.

Versions (checked on start.spring.io, 2026-10-12): **Spring Boot 4.1.1** (latest stable; 4.2 is still a milestone; supports Java 17-27). It pulls in Spring Kafka 4.1.1, kafka-clients 4.2.1, Hibernate 7.4, Flyway 12.4, Jackson 3.1.
Build tool: **Maven** with the `mvnw` wrapper (no global install, the wrapper downloads the pinned Maven version). Chosen over Gradle because Spring Initializr, docs and most Spring examples default to it and the XML is easy to read; Gradle builds faster but its Kotlin/Groovy DSL is one more thing to learn.

Boot 4 changes we ran into: starters are now modular (`spring-boot-starter-webmvc`, `spring-boot-starter-flyway`, `spring-boot-starter-kafka`), and Jackson 3 moved packages from `com.fasterxml.jackson` to `tools.jackson`.

### 🔎 HOW IT WORKS: Flyway
- What it is:      A database migration tool: versioned SQL files that build the schema step by step.
- What it does:    On startup it ran `V1__create_orders.sql` and logged `Successfully applied 1 migration ... now at version v1`.
- How it does it:  It keeps a table `flyway_schema_history` with every applied version and a checksum. On each start it runs only files with a higher version than the last applied one.
- Why we use it:   Schema changes are code, reviewed and versioned in Git, identical on every machine. Alternative: `ddl-auto=update` (Hibernate guesses the schema from entities: can't rename, never drops, unsafe in production). We set `ddl-auto=validate`, so Hibernate only *checks* that entities match the tables and fails fast if not.
- Common pitfall:  Never edit an applied migration. The checksum changes and Flyway refuses to start. Write a new `V2__...` instead.

### 🔎 HOW IT WORKS: `@Entity`, `@OneToMany(mappedBy)`, `@ManyToOne(LAZY)`
- What it is:      Annotations that map Java classes to tables and object references to foreign keys.
- What it does:    `Order` ↔ `orders`, `OrderItem` ↔ `order_items`. `order.getItems()` returns the order's items; `item.order` points back.
- How it does it:  Only one side "owns" a relationship: the side with the foreign key column. `OrderItem.order` (`@ManyToOne @JoinColumn(name="order_id")`) is the owner. `Order.items` (`@OneToMany(mappedBy="order")`) is the inverse side: a read-only mirror, it does not write the FK. `cascade=ALL` means saving an Order also saves its items; `orphanRemoval` deletes an item removed from the list.
- Why we use it:   Natural object model with one FK. Without `mappedBy`, JPA would think both sides own the relation and create an extra join table.
- Common pitfall:  `@ManyToOne` and `@OneToOne` default to **EAGER**; `@OneToMany` and `@ManyToMany` default to LAZY. Always write `fetch = FetchType.LAZY` on `@ManyToOne` explicitly.

### 🔎 HOW IT WORKS: `FetchType.LAZY`
- What it is:      "Don't load this relationship until someone touches it."
- What it does:    Loading an Order runs one `select ... from orders`. Items are loaded only when `getItems()` is first used.
- How it does it:  Hibernate puts a placeholder (a proxy, or a special collection wrapper) in the field. On first access it runs the SQL, but only if the session (persistence context) is still open.
- Why we use it:   EAGER loads data you often don't need, can't be turned off per query, and silently multiplies joins. LAZY + an explicit fetch where you need the data (see `@EntityGraph`) gives control.
- Common pitfall:  Touching a lazy field after the session is closed → `LazyInitializationException`. And touching it in a loop → N+1.

### 🔎 HOW IT WORKS: Persistence context and `@Transactional`
- What it is:      The persistence context is Hibernate's per-transaction memory of loaded entities (a first-level cache + change tracker). `@Transactional` tells Spring to open a DB transaction (and that context) around a method.
- What it does:    Inside `get()` and `findByStatus()` (`@Transactional(readOnly = true)`), lazy items can be loaded because the context is open. When the method returns, the transaction commits and the context closes.
- How it does it:  Spring wraps the bean in a proxy. The proxy starts the transaction, calls your method, then commits (or rolls back on a `RuntimeException`). Inside, the same entity id always returns the same Java object, and on commit Hibernate compares entities to their snapshot and writes changes automatically ("dirty checking": no `save()` needed to update a loaded entity).
- Why we use it:   One clear boundary: all reads/writes in a use case are atomic. `readOnly = true` skips dirty checking (cheaper) and documents intent.
- Common pitfall:  The proxy only works on calls *from outside* the bean. `this.someTransactionalMethod()` inside the same class bypasses it, and no transaction is started. Also: `create()` is deliberately **not** `@Transactional` (see "Publishing after commit" below).

### 🔎 HOW IT WORKS: `save` vs `saveAndFlush`
- What it is:      Two Spring Data repository methods for storing an entity.
- What it does:    `save()` registers the entity with the persistence context; the `INSERT` may run later. `saveAndFlush()` sends the SQL to the DB right now (still inside the transaction, not committed).
- How it does it:  Hibernate queues SQL and "flushes" it at commit, or before a query that needs fresh data. `flush()` forces that immediately. For a new entity (id still `null`) `save()` calls `em.persist()`; for an entity with an id set, it calls `em.merge()`, which first does a `SELECT` to check whether the row exists. That's why we let Hibernate generate the UUID (`GenerationType.UUID`) instead of setting it ourselves in Java.
- Why we use it:   `save()` is enough almost always. Use `saveAndFlush()` when you need DB errors (unique/check constraint violations) to surface at that exact line, or another query/process must see the row before the transaction ends.
- Common pitfall:  Thinking `saveAndFlush()` commits. It doesn't: a later exception still rolls it back.

Side note: `OrderItem` uses `GenerationType.IDENTITY` (DB generates the id). Hibernate must run each `INSERT` immediately to learn the id, which disables JDBC batch inserts. Fine at our scale; use a `SEQUENCE` for bulk inserts.

### 🔎 HOW IT WORKS: `spring.jpa.open-in-view=false`
- What it is:      Boot's default (`true`) keeps the persistence context open for the whole HTTP request, including JSON rendering in the controller.
- What it does:    With `false`, lazy loading only works inside service `@Transactional` methods. That's why DTOs (`OrderResponse.from`) are built inside the service.
- How it does it:  Open-in-view registers a web interceptor that opens the session at the start of the request and closes it at the end.
- Why we use it:   With it on, the controller or Jackson can trigger lazy queries during rendering: N+1 hidden in the view layer, and a DB connection held for the whole request. Off = data access stays in the service layer where you can see it.
- Common pitfall:  After turning it off, returning entities from controllers throws `LazyInitializationException`. Return DTOs.

### 🔎 HOW IT WORKS: Bean Validation and `422`
- What it is:      Annotations (`@NotBlank`, `@NotEmpty`, `@Positive`, `@Valid`) checked automatically on `@Valid @RequestBody`.
- What it does:    `{"customerId":"","items":[{"sku":"SKU-1","quantity":0}]}` → `422` with `errors: {"customerId":"must not be blank","items[0].quantity":"must be greater than 0"}`.
- How it does it:  Spring parses JSON into the record, runs the Hibernate Validator engine, and throws `MethodArgumentNotValidException` on failure. `List<@Valid Item>` makes it validate each item too.
- Why we use it:   `400 Bad Request` = the request could not be parsed (broken JSON still returns 400). `422 Unprocessable Content` = parsed fine, but the values break business rules. Clients can tell "my code is broken" from "my user typed something wrong". Spring returns 400 for both by default.
- Common pitfall (we hit it): with `spring.mvc.problemdetails.enabled=true`, Boot registers its own `ResponseEntityExceptionHandler` which handled the exception first, and our `@ExceptionHandler` never ran (`400` instead of `422`). Fix: extend `ResponseEntityExceptionHandler` and override `handleMethodArgumentNotValid`. Boot's handler backs off when one already exists.

### 🔎 HOW IT WORKS: ProblemDetail (RFC 9457)
- What it is:      A standard JSON error shape: `type`, `title`, `status`, `detail`, `instance`, plus custom fields.
- What it does:    All errors (400, 404, 422) come back as `application/problem+json` with the same shape.
- How it does it:  `ResponseEntityExceptionHandler` converts Spring MVC exceptions and `ResponseStatusException` into `ProblemDetail` bodies.
- Why we use it:   One error format for every client (and later the MCP server). Alternative: a custom `ErrorResponse` class: works, but non-standard.
- Common pitfall:  Putting exception messages or stack traces into `detail`: leaks internals. Keep it user-facing.

### 🔎 HOW IT WORKS: `KafkaTemplate`
- What it is:      Spring's helper for sending messages, like `RestTemplate`/`JdbcTemplate` but for Kafka.
- What it does:    `kafka.send("order.created", orderId, event)` → log `Published order.created ... -> partition 1 offset 0`.
- How it does it:  `send()` serializes key and value, picks the partition (`hash(key) % partitions`), puts the record in an in-memory batch, and returns a `CompletableFuture` immediately. A background I/O thread sends batches; the future completes when the broker acknowledges.
- Why we use it:   Handles producer creation, serialization and thread-safety from Boot config. We react to the result in `whenComplete` (log success or failure) instead of blocking the HTTP thread.
- Common pitfall:  Ignoring the future. A failed send is then completely silent. Also: if the broker is unreachable, `send()` itself can block up to `max.block.ms` (60s default) waiting for metadata.

### 🔎 HOW IT WORKS: Producer `acks=all`
- What it is:      How many broker replicas must store the message before the producer counts it as sent.
- What it does:    `acks=0`: don't wait (fast, can lose data). `acks=1`: leader stored it (lost if leader dies before copying). `acks=all`: all in-sync replicas stored it.
- How it does it:  The leader waits until followers in the ISR (in-sync replica set) have copied the record, then replies. With our single broker, "all" = 1 replica, but the setting is ready for a real cluster (where you'd also set `min.insync.replicas=2`).
- Why we use it:   An order event must not be lost. Cost: a bit more latency per batch.
- Common pitfall:  `acks=all` with `min.insync.replicas=1` still accepts writes when only the leader is alive. Durability needs both settings.

### 🔎 HOW IT WORKS: Idempotent producer (vs idempotent consumer)
- What it is:      `enable.idempotence=true`: the broker discards duplicate writes caused by producer retries.
- What it does:    If the ack for a batch is lost and the producer retries, the message is still written only once into the partition.
- How it does it:  The broker gives the producer an id; the producer numbers every batch per partition (sequence number). The broker remembers the last sequence per producer+partition and drops repeats.
- Why we use it:   Retries without duplicates and without reordering. Since Kafka 3.0 `acks=all` + idempotence are already the defaults; we set them explicitly so the intent is visible in config.
- Common pitfall:  It only protects against *producer retries within one producer session*. It does NOT stop the app from sending the same event twice (e.g. our code calls `send()` twice, or the app restarts and resends), and it does nothing for consumers that process a message twice after a crash. That's the **idempotent consumer** (Phase 2): the consumer remembers `eventId`s it has already handled.

### 🔎 HOW IT WORKS: JSON serialization (`JacksonJsonSerializer`)
- What it is:      Converts the `EventEnvelope` Java record into JSON bytes for Kafka. The key uses `StringSerializer`.
- What it does:    The event on the topic: `{"eventId":"52f8...","eventType":"order.created","occurredAt":"2026-10-12T13:22:22.37Z","orderId":"3704...","payload":{...}}`.
- How it does it:  Spring Kafka 4.x ships `JacksonJsonSerializer` (Jackson 3). The old `JsonSerializer` is the Jackson 2 version, deprecated. By default it also adds a `__TypeId__` header holding the Java class name; we turned that off (`spring.json.add.type.headers=false`).
- Why we use it:   Human-readable on the CLI, no extra infrastructure. Alternative: Avro/Protobuf + schema registry (typed, versioned contracts), out of scope here. Without type headers, other services aren't tied to `com.example.platform.order.EventEnvelope`; they map the JSON to their own classes. The contract is the JSON, not the Java class.
- Common pitfall:  Consumers trusting the type header blindly (security risk: deserializing to arbitrary classes) or breaking when the producer renames a class.

### 🔎 HOW IT WORKS: `NewTopic` bean
- What it is:      Declares a topic in code.
- What it does:    On startup `KafkaAdmin` creates `order.created` with 3 partitions if it doesn't exist (no-op if it does).
- How it does it:  Boot auto-configures `KafkaAdmin`, which collects all `NewTopic` beans and calls Kafka's AdminClient.
- Why we use it:   Partition count is explicit and versioned. Alternative: broker auto-create on first send (uses broker defaults, easy to get wrong).
- Common pitfall:  Changing `partitions(3)` to fewer later does nothing (Kafka can't shrink partitions).

### 🔎 HOW IT WORKS: Publishing after commit (first look at the dual-write problem)
- `create()` is not `@Transactional`. `orders.save()` runs in its own transaction and commits on return, then we publish.
- If we published *inside* a transaction and the commit then failed, inventory would receive an event for an order that doesn't exist (a ghost order).
- Our choice: if Kafka is down after the commit, the order stays `PENDING` with no event. That's visible (the stuck-orders endpoint in Phase 3) and recoverable. The proper fix is the **transactional outbox** (Phase 6).

### Deliberate lesson: the N+1 problem
`GET /orders?status=PENDING` maps each order to a DTO, which calls `order.getItems()` (LAZY).

Before (4 orders → 5 queries):
```
select ... from orders o1_0 where o1_0.status=?
select ... from order_items i1_0 where i1_0.order_id=?
select ... from order_items i1_0 where i1_0.order_id=?
select ... from order_items i1_0 where i1_0.order_id=?
select ... from order_items i1_0 where i1_0.order_id=?
```

Fix: one annotation on the repository method:
```java
@EntityGraph(attributePaths = "items")
List<Order> findByStatus(OrderStatus status);
```

After (4 orders → 1 query):
```
select ... from orders o1_0 left join order_items i1_0 on o1_0.id=i1_0.order_id where o1_0.status=?
```

### 🔎 HOW IT WORKS: `@EntityGraph` (vs `JOIN FETCH`)
- What it is:      Tells Spring Data JPA which LAZY relationships to load in the same query for one repository method.
- What it does:    Turns 1+N queries into 1 query with a `LEFT JOIN`.
- How it does it:  Spring passes a "fetch graph" hint to Hibernate, which adds the join and fills the collections from the joined rows. Hibernate 6+ de-duplicates the parent rows automatically (an order with 3 items comes back as 3 rows but 1 `Order`).
- Why we use it:   Works with derived query methods (`findByStatus`), no JPQL to write. `JOIN FETCH` in an `@Query` does the same and is better when the query is custom anyway.
- Common pitfall:  Fetching a collection + pagination: Hibernate can't LIMIT joined rows correctly, so it loads everything and paginates in memory (warning `HHH90003004`). Also, never fetch two `List` collections in one query (`MultipleBagFetchException`).

### Check your understanding
1. `OrderItem.order` is `@ManyToOne(fetch = LAZY)` and `Order.items` uses `mappedBy`. Which side writes the `order_id` foreign key? What goes wrong if `fetch = LAZY` is removed from `@ManyToOne`?
2. Inside a `@Transactional` method you load an order with `findById` and change a field, never calling `save()`. Is the change written to the database? Why?
3. The producer has `enable.idempotence=true`. Does that stop inventory-service from reserving stock twice for the same order?

<details>
<summary>Answers</summary>

- **Owning side writes the FK.** `OrderItem.order` (`@ManyToOne` + `@JoinColumn`) owns `order_id`. JPA *ignores* the `mappedBy` side when writing: `order.getItems().add(item)` alone leaves `order_id` null. Always set the owning side; `Order.addItem()` does both in one place.
- **EAGER `@ManyToOne`** = every loaded `OrderItem` drags its `Order` along, needed or not; N+1 in the other direction when querying items, and no way to switch it off per query.
- **Dirty checking.** An entity loaded inside a transaction is *managed*: Hibernate snapshots it and, at commit, writes an `UPDATE` for changed fields. No `save()` needed. Outside a transaction it's *detached* and changes are not saved. Default rollback is only on `RuntimeException`; checked exceptions still commit unless `rollbackFor` is set.
- **Idempotent producer ≠ idempotent consumer.** The producer only removes duplicates from its own network retries in one session. App-level resends, restarts and consumer redelivery after a crash need consumer-side deduplication by `eventId` (Phase 2).

</details>


## Phase 2: Inventory service (consume + idempotency)

Goal: inventory-service consumes `order.created`, reserves stock all-or-nothing, publishes `inventory.reserved` / `inventory.rejected`. A duplicate event must have no second effect; concurrent orders must never oversell.

```
order.created ─► ReservationListener (3 threads, 1 per partition), one DB transaction:
                   1. eventId in processed_events?  yes → skip
                   2. check all items → reserve all (or none)
                   3. INSERT processed_events(eventId)
                   4. flush  (UPDATE stock ... WHERE version=?  → conflicts fail here)
                   5. send inventory.reserved|rejected, wait for ack
                 commit DB → container commits the offset
```

### 🔎 HOW IT WORKS: `@KafkaListener`
- What it is:      Marks a method to be called for every record on a topic. Like a `@PostMapping` method, but triggered by a Kafka record instead of an HTTP request.
- What it does:    On startup the log shows `inventory-service: partitions assigned: [order.created-0]` (and -1, -2), then one `Order ... -> inventory.reserved` line per record.
- How it does it:  Spring creates a *listener container* per listener: a background thread that loops `poll()` → call your method for each record → commit offsets. The method parameter type (`EventEnvelope<OrderCreated>`) tells the message converter what to build from the JSON.
- Why we use it:   No hand-written poll loop, thread management or offset bookkeeping. Alternative: the raw `KafkaConsumer` API (full control, lots of boilerplate, easy to get commits wrong).
- Common pitfall:  Exceptions don't "return 500" to anyone. A thrown exception goes to the container's **error handler**, which by default retries the record 9 more times (10 attempts, no backoff) and then logs and skips it. Phase 4 replaces "skip" with a dead-letter topic.

### 🔎 HOW IT WORKS: Message converter (`StringJacksonJsonMessageConverter`)
- What it is:      The piece that turns the raw record value into the listener's parameter type.
- What it does:    Kafka hands over a `String` (`StringDeserializer`), the converter maps the JSON to `EventEnvelope<OrderCreated>`.
- How it does it:  Boot finds the `RecordMessageConverter` bean and plugs it into the listener container factory. The converter reads the generic method signature, so `EventEnvelope<OrderCreated>` works without type headers.
- Why we use it:   inventory-service has its own `Events` records; it does not depend on order-service's Java classes, only on the JSON contract. Alternative: `JacksonJsonDeserializer` in the consumer config with `spring.json.value.default.type`; then a malformed message fails *inside* `poll()`, before your code, which needs an extra `ErrorHandlingDeserializer` wrapper to handle it.
- Common pitfall:  A field renamed on the producer side becomes `null` on the consumer side, silently. Contract changes must be additive (add fields, never rename/remove) or versioned.

### 🔎 HOW IT WORKS: Listener concurrency
- What it is:      `spring.kafka.listener.concurrency=3`: three consumer threads in this one app, all in group `inventory-service`.
- What it does:    Each thread got one partition (`ntainer#0-0`, `#0-1`, `#0-2` in logs). Records from different partitions are processed in parallel; records within one partition strictly one after another.
- How it does it:  Each thread is a separate Kafka consumer in the same group, so the group assigns partitions to them exactly as it would to separate instances.
- Why we use it:   Use all 3 partitions without running 3 copies of the service. With 2 instances × 3 threads, 3 threads would sit idle (6 consumers, 3 partitions).
- Common pitfall:  Concurrency makes shared data races real: two threads can touch the same `stock` row at once. Hence `@Version`.

### 🔎 HOW IT WORKS: Ack modes and committing offsets after processing
- What it is:      *When* the container commits "I'm done up to offset X" for the group.
- What it does:    `ack-mode=record`: commit after each record whose listener call returned normally. Spring's default is `BATCH`: commit once after all records from one `poll()` are done.
- How it does it:  Spring sets Kafka's `enable.auto.commit=false` and commits itself. (Kafka's own auto-commit commits on a timer, every 5s, regardless of whether processing finished.) If the method throws, nothing is committed for that record.
- Why we use it:   **Commit after processing = at-least-once**: a crash can only cause a redo, never a skip. Committing *before* processing = at-most-once: a crash after commit loses the message. `RECORD` instead of `BATCH` makes the redo window one record instead of up to a whole poll (default up to 500 records); it costs one commit request per record, which is fine at our volume.
- Common pitfall:  Kafka auto-commit (`enable.auto.commit=true`) with slow processing: the timer commits offsets for records still being processed. A crash then loses them.

### 🔎 HOW IT WORKS: At-least-once delivery
- What it is:      The guarantee "every message is processed, possibly more than once".
- What it does:    A record is redelivered if the consumer crashes after doing the work but before the offset commit, or during a rebalance (partition moved to another consumer before the commit).
- How it does it:  It's the natural result of "do work, then commit offset" + "on restart, resume from last committed offset".
- Why we use it:   Of the three options, at-most-once loses data and exactly-once is expensive and only end-to-end within Kafka (Kafka transactions: consume + produce + offset commit atomic; our *database* write is still outside that transaction). At-least-once + an idempotent consumer gives "effectively once" for our DB effects.
- Common pitfall:  Assuming "it worked in testing so duplicates don't happen". They happen exactly in the rare situations (deploys, rebalances, crashes) where you aren't watching.

### 🔎 HOW IT WORKS: Idempotent consumer (`processed_events`)
- What it is:      A table of `eventId`s already handled. `eventId` is the primary key.
- What it does:    Re-publishing a real event byte-for-byte → log `Duplicate event 52f839c7-... for order 3704147b-..., skipping`; SKU-1 stayed at 95.
- How it does it:  In ONE database transaction: check `existsById(eventId)` → do the stock change → insert the `eventId`. Either both the stock change and the marker are committed, or neither. If two deliveries of the same event race (both pass the check), the second `INSERT` hits the primary key at commit and its whole transaction rolls back.
- Why we use it:   At-least-once delivery makes duplicates certain over time. "Processing twice has the same effect as once" is the only robust answer. Alternative: make the operation naturally idempotent (e.g. `SET reserved_for_order = X` instead of `available - 1`), not possible for every operation.
- Common pitfall:  Writing the marker in a *separate* transaction (or a different store like Redis). A crash between the two writes leaves "marked but not done" (lost work) or "done but not marked" (double work). Same transaction, same database.

Detail we ran into: `processedEvents.save(new ProcessedEvent(id))` with an *assigned* id makes Spring Data call `merge()`, not `persist()` (it can't tell new from existing when the id is already set, see Phase 1 `save` notes). `merge()` does a `SELECT` first and would silently *update* an existing row instead of failing. That's why the explicit `existsById` check comes first, with the primary key as the safety net for the race.

### 🔎 HOW IT WORKS: `@Version` (optimistic locking) vs pessimistic locking
- What it is:      A version column that Hibernate checks and increments on every update of the row.
- What it does:    Two transactions read SKU-3 (`available=1, version=5`). The first commits `available=0, version=6`. The second sends `UPDATE stock SET available=0, version=6 WHERE sku='SKU-3' AND version=5` → `UPDATE 0` → `ObjectOptimisticLockingFailureException` → rollback → the error handler re-runs the record → it now reads `available=0` → rejected.
- How it does it:  No locks are held while you work. The conflict is detected at write time by the `WHERE version = ?` condition. We saw the mechanism directly: a stale-version UPDATE in psql returned `UPDATE 0`.
- Why we use it:   Conflicts on the same SKU are rare, retries are cheap, nobody waits. **Pessimistic** locking (`SELECT ... FOR UPDATE`, `@Lock(PESSIMISTIC_WRITE)`) locks the row on read so the second transaction *waits*: better when conflicts are frequent (one hot product in a flash sale), but threads block and deadlocks become possible.
- Common pitfall:  Catching the exception and continuing. The whole transaction is dead; the operation must be retried from the start with fresh data (here: by the Kafka error handler re-running the record).

Experiment: 10 parallel orders × 1 unit of SKU-3 (stock 5) → exactly 5 reserved, 5 rejected, SKU-3 = 0, version 5. No optimistic-lock conflict appeared in this run (processing took milliseconds and the threads did not overlap; retries are silent at INFO level). The correct total proves safety, not the collision; Phase 5 adds a deterministic test that forces two threads to collide.

### 🔎 HOW IT WORKS: `flush()` before publishing
- What it is:      Forcing Hibernate to send pending SQL (the `UPDATE ... WHERE version=?`) before the method ends.
- What it does:    A version conflict throws at `stocks.flush()`, *before* `kafka.send()`.
- How it does it:  Without it, Hibernate sends the UPDATE at commit, which is *after* we published. A conflict then would roll back the reservation but `inventory.reserved` would already be out, and the retry would publish `inventory.rejected` for the same order: two contradictory events.
- Why we use it:   Publish only what the database has already accepted (except for the commit itself). This is the Phase 1 `save` vs `saveAndFlush` point in practice.
- Common pitfall:  Thinking `flush()` commits. It doesn't. Commit can still fail (e.g. DB down), which leaves the remaining small gap: result published, reservation rolled back, redelivery publishes again. We accept a possible *duplicate* result because order-service will be idempotent too; a *lost* result would leave the order stuck. The full fix is again the transactional outbox (Phase 6).

### 🔎 HOW IT WORKS: All-or-nothing reservation
- An order with several items is checked completely before any stock changes. Experiment: `SKU-1 ×10` (available) + `SKU-4 ×1` (out of stock) → rejected, SKU-1 stayed at 95.
- The same SKU twice in one order is summed first, so the check is against the combined quantity.
- `CHECK (available >= 0)` in the schema is the last line of defence if a code path ever forgets the check.

### 🔎 HOW IT WORKS: `auto-offset-reset=earliest`
- What it is:      Where a consumer group starts when it has **no committed offset** yet.
- What it does:    Our brand-new group `inventory-service` processed the 4 orders created in Phase 1 before inventory-service existed.
- How it does it:  `earliest` = start of the partition, `latest` (Kafka's default) = only messages produced from now on. Once the group has committed offsets, this setting is ignored.
- Why we use it:   A new service shouldn't silently miss events produced before its first start.
- Common pitfall:  Changing `group-id` (e.g. renaming the service) with `earliest` = the "new" group reprocesses the whole topic. With `latest` = it silently skips everything in between.

### Check your understanding
1. Suppose the `processed_events` marker were saved in a **separate** transaction committed first, and the service crashed right after. What happens when the event is redelivered?
2. With `ack-mode=record`, when exactly is the offset committed: before the listener runs, after it returns normally, or after it throws? Why does that give at-least-once?
3. Two orders for the last unit of a SKU are processed at the same moment. Walk through what `@Version` does. When would you pick pessimistic locking instead?

<details>
<summary>Answers</summary>

- **Why the marker must share the stock transaction.** Marker committed in its own transaction, then crash before the stock change: the offset of `order.created` was never committed, so Kafka redelivers that same record; `existsById` says "done" → skipped → no reservation, no `inventory.*` event, order stuck `PENDING` forever (lost work). Reverse order (stock first, marker later) → crash in between → redelivery reserves stock twice. One transaction avoids both.
- **At-least-once = commit only after success.** Any crash or exception before the commit leaves the offset unchanged → redelivery → never skipped, possibly repeated. Commit before processing = at-most-once (loss). Committing after a failure = silently skipping failed messages (also loss).
- **`@Version` detects, it doesn't prevent.** The stale read still happens; the `WHERE version = ?` makes the write fail (0 rows) so the work is redone with fresh data.
- **When pessimistic (`SELECT ... FOR UPDATE`) wins:** high contention on a hot row (optimistic → retry storms, possible starvation); non-repeatable side effects inside the transaction; expensive retries. Cost: blocked threads, deadlock risk, worse with long transactions.
- **Third option: atomic conditional update.** `UPDATE stock SET available = available - ? WHERE sku = ? AND available >= ?`: no read-modify-write, no retry; 1 row = reserved, 0 = rejected. Great for single counters, awkward for multi-item all-or-nothing.

</details>


## Phase 3: Complete the saga

Goal: order-service consumes `inventory.reserved` / `inventory.rejected`, moves the order to `CONFIRMED` / `CANCELLED`, records every status change, and publishes `order.confirmed` / `order.cancelled`. New endpoints: `GET /orders/{id}/timeline`, `GET /orders?status=&olderThanMinutes=`.

```
client        order-service                 Kafka                    inventory-service
  │ POST /orders  │                            │                            │
  │──────────────►│ save PENDING (+history)    │                            │
  │◄── 201 PENDING│── order.created ──────────►│───────────────────────────►│ reserve (or not)
  │               │                            │◄── inventory.reserved ─────│
  │               │◄───────────────────────────│    or inventory.rejected   │
  │               │ PENDING→CONFIRMED|CANCELLED│                            │
  │               │── order.confirmed|cancelled►  (notification-service, Phase 4)
```

Observed: create → read immediately: `PENDING` → read 1 s later: `CONFIRMED` (≈ 42 ms between the two history entries).

### 🔎 HOW IT WORKS: The Saga pattern
- What it is:      A way to run one business operation across several services when there is no shared database transaction. It's a sequence of *local* transactions, each in one service, connected by events.
- What it does:    "Place an order" = order-service tx (save PENDING) → inventory-service tx (reserve) → order-service tx (confirm/cancel). Each step commits on its own.
- How it does it:  Each service does its local transaction and publishes an event; the next service reacts to it. If a later step fails, earlier steps are not rolled back automatically (they're already committed); instead a **compensating action** undoes them semantically.
- Why we use it:   Database-per-service rules out a single ACID transaction across `orders_db` and `inventory_db`. The alternative, distributed transactions (two-phase commit / XA), locks resources across services, needs every participant to support it (Kafka doesn't in that way) and makes everyone as available as the weakest participant.
- Common pitfall:  Forgetting that between steps the system is *in between*: an order is `PENDING` while stock is already reserved. Every reader must tolerate those intermediate states.

### 🔎 HOW IT WORKS: Compensating action
- What it is:      A new, forward-moving step that semantically undoes an earlier committed step. Not a rollback: the original change stays in history.
- What it does:    `inventory.rejected` → order goes `PENDING → CANCELLED`, with the reason in the timeline (`Not enough stock for SKU-4 (requested 1, available 0)`).
- How it does it:  The order was provisionally accepted (step 1 committed). Cancelling it is the compensation. Inventory needs no compensation in our flow because its reservation is all-or-nothing: a rejection changed nothing.
- Why we use it:   You can't un-commit another service's transaction, and you shouldn't delete history. A new state + a new event lets every other service react (e.g. notify the customer).
- Common pitfall:  Compensations must be idempotent and can't always restore the exact old world (an email already sent can't be unsent). If we later added a payment step that fails *after* reservation, the compensation would be a new `inventory.release` event to give the stock back.

### 🔎 HOW IT WORKS: Choreography vs orchestration
- What it is:      Two ways to coordinate a saga. **Choreography**: each service listens for events and decides itself what to do next; no one is in charge. **Orchestration**: one central orchestrator (a state machine) sends commands to each service ("reserve stock", "cancel order") and tracks the progress.
- What it does:    Ours is choreography: inventory-service doesn't know order-service exists; it just reacts to `order.created` and announces its result.
- How it does it:  The "flow" exists only as the sum of who-listens-to-what (the topic table in the brief).
- Why we use it:   Few steps (create → reserve → confirm/cancel), loose coupling, no extra component to build and run. Orchestration (e.g. a dedicated saga orchestrator, or tools like Temporal / Camunda) wins when there are many steps, branches, timeouts and compensations: the flow is then visible in one place and easier to change.
- Common pitfall:  With choreography the overall flow is invisible in code. It's spread across services, which is exactly why we added `/timeline` and will add the MCP tool `explain_order_timeline`. As steps grow, choreography turns into "event spaghetti" (cyclic dependencies, hard to answer "what happens after X?").

### 🔎 HOW IT WORKS: Eventual consistency
- What it is:      The guarantee that, if no new changes happen, all services will *eventually* agree on the state; not immediately.
- What it does:    Right after `POST /orders` the order is `PENDING` while inventory may already have reserved stock. ~40 ms later both agree. With inventory-service **stopped**, the order stayed `PENDING`; after restart it caught up from Kafka and became `CONFIRMED`. Nothing was lost.
- How it does it:  Kafka stores `order.created` until the consumer group reads it; offsets remember where inventory stopped.
- Why we use it:   The price of independent services. The alternative (strong consistency) needs synchronous calls/locks across services: inventory down = can't take orders. Here, inventory down = orders just take longer.
- Common pitfall:  APIs that hide the intermediate state. The client must be told "your order is PENDING" (201 + status) and poll or get notified, not assume success from the 201. Also: "eventually" needs monitoring. The stuck-orders query (`status=PENDING&olderThanMinutes=5`) is how we notice when "eventually" means "never" (e.g. the event was never published).

### 🔎 HOW IT WORKS: State-transition guard (+ idempotency, twice)
- `Order.complete()` only allows `PENDING → CONFIRMED` and `PENDING → CANCELLED`; anything else throws.
- The listener checks `status != PENDING` first: if another event already decided the order, it logs `Ignoring ...` and returns (no retry loop). Combined with `@Version` on `Order`, two contradictory results racing for the same order can't both win.
- `processed_events` handles exact duplicates (same `eventId`): replaying a real `inventory.reserved` → `Duplicate event 2a897023-... skipping`, timeline unchanged (2 entries). The status guard handles *different* events about the same order (e.g. the rare duplicate result inventory can publish, which has a new `eventId`).
- The marker is saved *before* the guard so that "ignored" events are also remembered.

### 🔎 HOW IT WORKS: Status history (timeline) and `@OrderBy`
- `order_status_history` gets one row per transition, in the same transaction as the status change. `PENDING` stores the `eventId` of the `order.created` we published (generated before saving, so it's known in the same transaction); later rows store the `eventId` of the inventory event that caused them. You can follow one order across services by these ids.
- `@OrderBy("occurredAt ASC, id ASC")` on `Order.history` makes Hibernate add `ORDER BY` when loading the collection, so the timeline is always chronological (id breaks ties within the same timestamp).
- Orders created before the migration got a backfilled `PENDING` row with `event_id = NULL` (`INSERT ... SELECT` in `V2`).
- Pitfall: a status history can drift from `orders.status` if someone updates one without the other. Keeping both changes behind `Order.complete()` avoids it.

### 🔎 HOW IT WORKS: Method validation on `@RequestParam`
- What it is:      Bean Validation annotations directly on controller parameters: `@RequestParam(defaultValue = "0") @PositiveOrZero int olderThanMinutes`.
- What it does:    `olderThanMinutes=-1` → `400 Validation failure`.
- How it does it:  Since Spring Framework 6.1, MVC validates constrained method parameters itself and throws `HandlerMethodValidationException`; `ResponseEntityExceptionHandler` turns it into a 400 problem detail.
- Why we use it:   No manual `if (x < 0)` in the controller. We kept it at 400 (bad URL) while body validation is 422; aligning both is a one-method override if wanted.
- Common pitfall:  On older Spring versions this needed `@Validated` on the controller class, otherwise the annotations were silently ignored.

### Note on renaming a payload
`OrderCreatedPayload` became `OrderPayload` because `order.confirmed` uses the same shape. Only the Java name changed; the JSON is identical, so no consumer is affected. That's the benefit of turning off type headers in Phase 1.

### Check your understanding
1. What is the compensating action in this flow, and why doesn't inventory give stock back on a rejection? If a payment step after the reservation fails, what compensation is needed and who does it?
2. Name one advantage of choreography here and one problem at 6-7 steps. What would orchestration do differently?
3. "The API said 201 but my order is still PENDING after 10 minutes." How do you investigate?

<details>
<summary>Answers</summary>

- **Our compensation is the cancellation.** Step 1 (save `PENDING`) is already committed; `PENDING → CANCELLED` undoes it semantically. Inventory needs none because its reservation is all-or-nothing.
- **With a payment step after reservation**, the compensation is "release stock", done by inventory-service on `payment.failed` / `order.cancelled`. That needs a `reservations(order_id, sku, quantity)` table (today inventory only decrements `available` and can't know what to give back) and an idempotent release.
- **Choreography**: loosely coupled (notification-service will be added without touching others). It breaks down with many steps: flow scattered across services, nobody owns timeouts, changes touch many listeners. **Orchestration**: a central state machine per order sends commands, owns timeouts and compensations; cost: an extra highly-available component everything depends on.
- **Stuck-order triage, hop by hop by eventId:** timeline (`PENDING` + `order.created` eventId) → was it published (log / topic)? not there = dual-write gap → inventory group lag (down or poison message) → inventory `processed_events` / `inventory.*` event → order-service group lag.

</details>


## Phase 4: Notification service, Redis, resilience, dead letters, Compose

Goal: the last service, a cache on stock reads, a protected synchronous call, a safe landing place for messages that keep failing, and `docker compose up` for the whole system.

New versions checked: `resilience4j-spring-boot4:2.4.0` (2026-03; the Maven Central *search* index still showed only the boot3 module, the repository metadata had boot4), Spring Data Redis 4.1.1 + Lettuce 7.5, `eclipse-temurin:21.0.12_8-jdk/-jre`.

### notification-service
- Consumes `order.confirmed` + `order.cancelled` (group `notification-service`), stores a row and logs `Notify cust-42: Your order ... is confirmed.`
- On first start it caught up with all 20 final events: 11 confirmed + 9 cancelled, exactly matching order-service.
- Idempotency without a `processed_events` table: `notifications.event_id` is `UNIQUE`, so the notification row **is** the marker. Same transaction by construction. Works whenever one event produces exactly one row.
- Added without touching order- or inventory-service: choreography's loose coupling in practice.

### 🔎 HOW IT WORKS: Cache-aside with `@Cacheable`
- What it is:      "Look in the cache first; on a miss, load from the database and put the result in the cache." `@Cacheable("stock")` does this around a method.
- What it does:    GET #1 → `No cache entry for key 'SKU-2'` + `Cache miss for SKU-2, reading database` + `Creating cache entry`. GET #2 → `Cache entry for key 'SKU-2' found`, the method body did not run. In Redis: key `stock::SKU-2` = `{"sku":"SKU-2","available":47}`, TTL 600 s.
- How it does it:  `@EnableCaching` makes Spring wrap the bean in a proxy (same idea as `@Transactional`). The proxy builds a key from the arguments, asks the `CacheManager`, and only calls the real method on a miss. Exceptions (unknown SKU → 404) are not cached.
- Why we use it:   Stock reads are frequent (every order pre-check, every UI page in a real shop); the DB is touched only on misses. Alternatives: read-through/write-through caches (the cache itself loads/writes the DB), or no cache and a read replica.
- Common pitfall:  Same as `@Transactional`: calling a `@Cacheable` method from inside the same class bypasses the proxy, so no caching happens. That's why the method lives in `StockService`, not in the controller or listener.

### 🔎 HOW IT WORKS: Cache invalidation (eviction after commit, TTL)
- What it is:      Removing a cache entry when the underlying data changes.
- What it does:    After a reservation of SKU-2, the key disappears; the next GET is a miss and returns the fresh value (47 → 44 → 42).
- How it does it:  `ReservationListener` calls `stockCache.evict(sku)` for every reserved SKU. The cache manager is built with `.transactionAware()`, so the eviction is **deferred until the DB transaction commits** (and skipped if it rolls back).
- Why we use it:   Evicting *before* commit has a race: evict → another request misses, reads the *old* committed row, caches it → our commit → stale value stays in cache. After-commit is much safer, but a smaller race still exists (a reader reads old row before our commit and writes it into the cache after our eviction). The **TTL (10 min)** is the upper bound for any staleness we didn't catch. Real stock decisions are made by the database in the reservation, never by the cache.
- Common pitfall:  We saw a stale value once in the demo, but not from the cache: my check ran 2 s *before* the reservation happened (the consumer was still joining its group after a restart). Before calling something a cache bug, check whether the write actually happened yet.

### 🔎 HOW IT WORKS: `RestClient` with timeouts
- What it is:      Spring's synchronous HTTP client (the modern successor of `RestTemplate`).
- What it does:    `GET http://inventory-service:8082/stock/{sku}`; 404 is mapped to "0 available" (a valid answer), other errors throw.
- How it does it:  Built from Boot's auto-configured `RestClient.Builder` with a JDK `HttpClient`: connect timeout 500 ms, read timeout 1 s.
- Why we use it:   Without timeouts, a hanging inventory-service would hold one of our HTTP threads per request until they're all used up, and order-service would hang too (cascading failure).
- Common pitfall (we hit it): Boot 4 moved the client auto-configuration into its own module. `spring-boot-starter-webmvc` no longer brings it; without `spring-boot-starter-restclient` startup fails with `required a bean of type 'RestClient$Builder' that could not be found`.

### 🔎 HOW IT WORKS: Circuit breaker (closed / open / half-open)
- What it is:      A switch in front of a remote call that stops calling a dependency that is clearly failing.
- What it does:    Observed with inventory-service stopped:
  ```
  order 1: 201 in 0.69s   (3 attempts: refused, 200ms, refused, 400ms, refused → fallback)
           Circuit breaker 'inventory': CLOSED to OPEN
  order 2: 201 in 0.006s  (CallNotPermittedException → fallback, no network call)
  order 3: 201 in 0.006s
  order 4: 201 in 0.004s
  ... inventory back, 15 s later ...
  order 5: OPEN to HALF_OPEN, order 5+6 succeed → HALF_OPEN to CLOSED
  ```
- How it does it:  **CLOSED**: calls go through; outcomes are recorded in a sliding window (last 10 calls). Once ≥ 4 calls are recorded and ≥ 50% failed → **OPEN**: every call fails immediately for 15 s. Then **HALF_OPEN**: 2 trial calls go through; if they succeed → CLOSED, if they fail → OPEN again. (It opened after order 1 because the earlier successful SKU-4 pre-check was call #1: 1 success + 3 failures = 75%.)
- Why we use it:   Fail fast instead of waiting for timeouts on every request; give the failing service room to recover instead of hammering it. Alternative: timeouts only (every request still pays the full timeout while the dependency is down).
- Common pitfall:  Counting "expected" answers as failures. A 404 for an unknown SKU is a correct answer from a healthy service; if it threw, a burst of bad SKUs would open the breaker for everyone. That's why the client maps 404 to a value instead of throwing.

### 🔎 HOW IT WORKS: Retry with exponential backoff (and annotation order)
- What it is:      Re-try a failed call a few times, waiting longer each time (200 ms, 400 ms).
- What it does:    Transient blips (a restart, one dropped connection) are absorbed; the caller doesn't notice.
- How it does it:  Resilience4j nests its decorators in a fixed order: `Retry( CircuitBreaker( call ) )`. Every attempt is recorded by the breaker. `CallNotPermittedException` is in `ignore-exceptions`: when the breaker is open, retrying is pointless.
- Why we use it:   Backoff spreads retries out; immediate retries against an overloaded service make the overload worse (retry storm).
- Common pitfall:  Putting `fallbackMethod` on `@CircuitBreaker`. The breaker (inner) would catch the exception and return the fallback value, so Retry (outer) only ever sees success and never retries. The fallback belongs on the outermost decorator (`@Retry`).

### 🔎 HOW IT WORKS: Fallback and the sync + async trade-off
- What it is:      What we return when the protected call fails for good.
- What it does:    `Stock pre-check for SKU-1 skipped: ...ResourceAccessException` → the order is accepted as `PENDING`; the async saga decides. All 4 orders accepted that way were `CONFIRMED` once inventory came back.
- How it does it:  `inventoryUnavailable(String sku, Throwable cause)` returns `Optional.empty()` = "no opinion"; the pre-check only rejects on a definite "not enough".
- Why we use it:   The sync call is an optimisation (fast `422` instead of `201` → later `CANCELLED`), never a requirement. Mixing sync and async this way: + better UX for obviously impossible orders; − a runtime dependency between services, and the answer may already be stale (it even comes from the Redis cache). The async reservation stays the single source of truth.
- Common pitfall:  Letting the sync call become mandatory ("inventory down → can't take orders"). Then you've built a distributed monolith: two services that are only up when both are up.

### Timeouts vs retries vs bulkheads
- **Timeout**: bounds how long *one* call may take. Always first; everything else builds on it.
- **Retry**: re-runs failed calls; helps with *transient* errors, hurts with *overload*. Only for idempotent operations (a GET is safe; a "charge card" POST is not).
- **Circuit breaker**: stops calling a dependency that keeps failing; protects both sides.
- **Bulkhead**: limits how many concurrent calls may go to one dependency (e.g. max 10 threads for inventory), so one slow dependency can't use up every thread and starve unrelated endpoints. Not added here: one dependency, low traffic.

### 🔎 HOW IT WORKS: `DefaultErrorHandler` + dead-letter topic
- What it is:      What a listener container does when your `@KafkaListener` throws. Our bean: retry 3 times with backoff 1 s, 2 s, 4 s, then hand the record to a `DeadLetterPublishingRecoverer` that copies it to `<topic>.DLT` and commits the offset.
- What it does:    Measured with two poison messages to `order.created`:
  - malformed JSON (`this is {not json`) → in `order.created.DLT` after **0.5 s** (no retries)
  - valid JSON without `payload` (NPE in the listener) → in `order.created.DLT` after **7.6 s** (= 1 + 2 + 4 s of backoff)
  - both values byte-for-byte identical to the original; headers like `kafka_dlt-original-topic`, `-original-offset`, `-exception-message` describe what failed.
- How it does it:  Retries are **blocking**: the handler re-seeks the partition and waits, so later records in *that* partition wait too (that's what keeps per-key order). Some exceptions are classified as not retryable (conversion/deserialization errors, `ClassCastException`, ...) and go to the recoverer immediately. The DLT record goes to the same partition number as the original.
- Why we use it:   Without it, Spring's default retries 10 times and then *logs and skips* the record: the data is gone. With a DLT, the partition isn't blocked forever and nothing is lost; a human (or a tool) can inspect and replay. Alternative: non-blocking retry topics (`@RetryableTopic`: failed records go to `topic-retry-1s`, `-retry-2s`... so the main partition keeps flowing, at the cost of per-key ordering).
- Common pitfalls (we hit two):
  1. **Topic name.** Spring Kafka 4's default DLT name is `<topic>-dlt`. Our contract says `<topic>.DLT`, so we pass an explicit destination resolver `(record, ex) -> new TopicPartition(record.topic() + ".DLT", record.partition())`. The first run auto-created `order.created-dlt`.
  2. **Double encoding.** Our listeners receive the value as a `String`. The default `KafkaTemplate` uses `JacksonJsonSerializer`, which would turn that String into a JSON string (`"{\"eventId\"..."`). The DLT template overrides the value serializer with `StringSerializer`.
  - Also: the handler is **silent** on success: no log line per retry, none for the DLT publish. DLT topics need monitoring/alerting, or failures go unnoticed.

### 🔎 HOW IT WORKS: Multi-stage Dockerfile
- What it is:      One Dockerfile with two `FROM`s: a **build** stage (JDK + Maven wrapper → jar) and a **runtime** stage (JRE + jar only).
- What it does:    `services/Dockerfile` is shared by all three services; Compose passes each service folder as the build context.
- How it does it:  `COPY --from=build` takes only the jar into the final image. `RUN --mount=type=cache,target=/root/.m2` keeps the Maven repository between builds (BuildKit cache), so dependencies are downloaded once (the first build took 4.5 min, mostly downloads). The app runs as a non-root `app` user.
- Why we use it:   No JDK, Maven or source code in the runtime image: smaller attack surface; reproducible builds that don't depend on what's installed on the host. Alternative: `./mvnw spring-boot:build-image` (Cloud Native Buildpacks, no Dockerfile, but large builder images and less visible).
- Common pitfall:  Copying the whole folder before downloading dependencies without a cache mount: every source change re-downloads everything.

### 🔎 HOW IT WORKS: Services in Compose
- Containers reach each other by **service name** (`postgres`, `inventory-service`). Kafka is reached at `kafka:29092`, the INTERNAL listener we prepared in Phase 0; `localhost:9092` inside a container would mean the container itself.
- Spring reads environment variables as properties ("relaxed binding"): `SPRING_DATASOURCE_URL` overrides `spring.datasource.url`, `INVENTORY_BASE_URL` overrides `inventory.base-url`. Same jar, different config per environment.
- `x-spring-service` + YAML anchors (`&spring-env`, `<<: *spring-env`) share the common settings instead of copying them three times.
- `depends_on: condition: service_healthy` waits for Kafka/Postgres/Redis health checks. order-service deliberately does **not** depend on inventory-service: the breaker + fallback handle its absence, and the startup order shouldn't hide that.
- Pitfall: ports 8081-8083 can't be used by both a local `mvnw spring-boot:run` and the containers. Stop one before starting the other.

### Check your understanding
1. Why does `fallbackMethod` go on `@Retry` and not on `@CircuitBreaker`? What would logs and timings show otherwise?
2. Malformed JSON reached the DLT in 0.5 s, the NPE message after 7.6 s. Why the difference, and what happened to other orders in the same partition meanwhile?
3. We evict the cache after commit and still keep a 10-minute TTL. Describe a sequence where a stale value still ends up in Redis. Why is that acceptable?

<details>
<summary>Answers</summary>

- **Breaker ≠ retry.** The breaker decides whether a call is *allowed*; retry decides whether to *repeat* it. Nesting is `Retry( CircuitBreaker( call ) )`. A fallback on the inner breaker swallows every failure → Retry only sees success → no retries ever: order 1 would take a few ms (1 attempt) instead of 0.69 s, and the breaker would need 4 separate requests to open. Fallback goes on the outermost decorator.
- **Non-retryable = deterministic.** Conversion/deserialization errors fail identically every time, so they skip straight to the DLT. Blocking retries cause **head-of-line blocking**: unrelated orders whose key hashes to the same partition wait; other partitions keep flowing. Non-blocking retry topics avoid that but give up per-key ordering.
- **The remaining cache race:** reader misses and reads 47 → writer commits 44 and evicts (nothing there yet) → reader caches 47 → stale until TTL. Safe for reservations (DB + `@Version` decide). At the pre-check: stale-high = harmless (async cancel later); stale-low = a false `422` (e.g. after a restock). Any future restock path must evict as well.
- Cleanup: deleted the wrongly named `order.created-dlt` topic created by the first DLT run.

</details>


## Phase 5: Tests

Goal: tests that start their own real infrastructure, so `./mvnw test` passes on any machine with Docker (and later in CI), plus fast unit tests for the business rules.

Result: **20 tests, all green, with the Compose stack stopped** (0 project containers running).

| Service | Unit tests | Integration tests (Testcontainers) | Wall time |
|---|---|---|---|
| order-service | `OrderTest` (4): state transitions; `OrderServiceTest` (3): pre-check with Mockito | `OrderSagaIntegrationTest` (3): happy path, rejection, duplicate result | 15 s |
| inventory-service | `StockTest` (5): reserve, all-or-nothing check | `ReservationIntegrationTest` (4): reserved, rejected, duplicate event, `@Version` collision | 13 s |
| notification-service | – | `NotificationIntegrationTest` (1): duplicate event → one notification | 11 s |

The old `contextLoads` smoke tests were deleted: they only passed while the Compose stack happened to be running on localhost.

### 🔎 HOW IT WORKS: Unit vs integration tests
- What it is:      A **unit test** checks one class in isolation (plain `new`, no Spring, no I/O). An **integration test** checks real parts working together (Spring context + database + Kafka).
- What it does:    `StockTest` runs 5 tests in 5 ms. `ReservationIntegrationTest` needs ~10 s, mostly container startup.
- How it does it:  Unit tests construct objects directly (`new Stock("SKU-1", 5)`); integration tests use `@SpringBootTest`, which starts the full application context.
- Why we use it:   Business rules (state transitions, shortage checks) are cheapest to test as units: fast, precise failures. Things that only exist at the boundaries (transactions, Kafka delivery, SQL, optimistic locking, JSON contracts) can only be tested for real in integration tests.
- Common pitfall:  Testing everything through `@SpringBootTest` (slow suite nobody runs), or mocking everything (fast suite that passes while production breaks).

### 🔎 HOW IT WORKS: Mockito
- What it is:      A library that creates fake implementations (*mocks*) of classes/interfaces for unit tests.
- What it does:    `OrderServiceTest` mocks `OrderRepository`, `KafkaTemplate` and `InventoryClient`. `when(inventory.availableStock("SKU-1")).thenReturn(Optional.of(1))` scripts an answer; `verify(orders, never()).save(any())` checks that the 422 path saves nothing.
- How it does it:  It generates a subclass at runtime whose methods record calls and return scripted values (default: `null`, `0`, empty `Optional`/collections). `thenAnswer` runs code instead, here simulating Hibernate assigning an id on `save`.
- Why we use it:   Test the pre-check logic without a database, Kafka or a running inventory-service. In the integration test, `@MockitoBean InventoryClient` replaces just that one bean in the Spring context.
- Common pitfalls: (1) Mocking what you don't own and then testing the mock instead of the behaviour. Kafka and Postgres are tested for real instead. (2) We hit one: `call.getArgument(0)` is generic, and `ReflectionTestUtils.setField` has an overload taking `Class<?>`, so Java inferred `Class` → `ClassCastException`. Assigning to a typed variable first fixed it. (3) Warning seen: `Mockito is currently self-attaching ... will no longer work in future releases of the JDK`. Harmless on JDK 21; the fix (Surefire `argLine` with `-javaagent:` pointing at mockito-core) is deferred until we move past 21.

### 🔎 HOW IT WORKS: Testcontainers + `@ServiceConnection`
- What it is:      A library that starts throwaway Docker containers from test code. `@ServiceConnection` (Spring Boot) wires the app to them.
- What it does:    `TestcontainersConfiguration` declares `KafkaContainer("apache/kafka:4.3.1")`, `PostgreSQLContainer("postgres:18.6")` and (inventory) a Redis `GenericContainer`, the same tags as Compose. Tests ran with the Compose stack stopped.
- How it does it:  Each container is a Spring bean; Testcontainers starts it on a random free host port. `@ServiceConnection` creates `ConnectionDetails` beans that override `spring.datasource.url`, `spring.kafka.bootstrap-servers`, Redis host/port. Flyway then migrates the fresh database exactly as in production. A small helper container (Ryuk) removes everything when the JVM exits, even after a crash.
- Why we use it:   For messaging especially, mocks lie: they can't reproduce serialization, partitions, consumer groups, offsets, redelivery, transactions or optimistic locking. An embedded/in-memory substitute (H2 instead of Postgres, `@EmbeddedKafka`) behaves differently in exactly the details we care about. Real containers = the same behaviour as production. Cost: needs Docker and ~10 s startup.
- Common pitfall:  Using `:latest` image tags (Initializr's default). Tests then change behaviour when an image updates. Pin the same versions as production.
- Note: Boot 4.1 manages **Testcontainers 2.0.5**, which renamed the artifacts (`testcontainers-kafka`, `testcontainers-postgresql`) and packages (`org.testcontainers.kafka.KafkaContainer`, `org.testcontainers.postgresql.PostgreSQLContainer`).

### 🔎 HOW IT WORKS: Spring test-context caching
- What it is:      Spring keeps an application context (and its container beans) alive and reuses it for every test class with the same configuration.
- What it does:    Containers start once per test run, not once per test or per class.
- How it does it:  The cache key is the full test configuration (`@SpringBootTest` settings, `@Import`s, `@MockitoBean`s, properties...).
- Why we use it:   Startup is the expensive part. Each different combination (e.g. adding a `@MockitoBean` to one class only) creates another context and another set of containers.
- Common pitfall:  Many slightly different test configurations → many contexts → slow suites and lots of containers alive at once.

### 🔎 HOW IT WORKS: Waiting for async results (Awaitility, no `Thread.sleep`)
- What it is:      A library that polls a condition until it is true or a timeout expires.
- What it does:    `await().atMost(15s).pollInSameThread().untilAsserted(() -> assertThat(available()).isEqualTo(3))` passes as soon as the listener has done its work, usually well under a second.
- How it does it:  Re-runs the assertion every 100 ms; the last assertion error is reported if it never passes. `pollInSameThread()` because our test `KafkaConsumer` must not be used from another thread.
- Why we use it:   `Thread.sleep(2000)` is both too slow (always waits the full time) and flaky (sometimes 2 s isn't enough on a busy CI runner).
- Common pitfall:  Proving that something does **not** happen (no second reservation). "Not yet" looks the same as "never". `await().during(3s)` requires the condition to *stay* true for 3 s: a bounded, explicit window. It can't prove "never", only "not within 3 s after the first effect", which is good enough because the duplicate is consumed right behind the original.

### Test isolation
- Each inventory test inserts its own `TEST-<uuid>` SKU and each order test creates its own order, so tests can run in any order on a shared database and never see each other's changes. No cleanup step, no `@DirtiesContext`.
- Each test reads results with its **own consumer group** (`test-<uuid>`, `earliest`), filtered by key = orderId, so it sees only its own events.

### Deterministic optimistic-lock collision (the test owed since Phase 2)
```java
outer.executeWithoutResult(tx -> {
    Stock mine = stocks.findById(sku).orElseThrow();                                    // A reads version 0
    inner.executeWithoutResult(other -> stocks.findById(sku).orElseThrow().reserve(1)); // B (REQUIRES_NEW) commits version 1
    mine.reserve(1);                                                                    // A: UPDATE ... WHERE version = 0
});  // → ObjectOptimisticLockingFailureException at A's commit; available = 4 (only B happened)
```
`PROPAGATION_REQUIRES_NEW` suspends A's transaction and runs B in a new one with its own persistence context, a second "user" in the same thread. No timing, no threads, no flakiness.

### Testing the tests
Temporarily replacing the `existsById` duplicate check with `if (false)` made `duplicateEventHasNoSecondEffect` fail with `expected: 3 but was: 1` (stock reserved twice). A test that can't fail proves nothing; this one can.

### Check your understanding
1. Why test Kafka and Postgres with real containers instead of mocking `KafkaTemplate` and the repositories? Name a bug a mock-based test would have missed.
2. Why can't a plain `await().until(...)` prove that a duplicate was ignored, and why not `Thread.sleep(3000)` followed by an assert?
3. What does `PROPAGATION_REQUIRES_NEW` do, and why does it make the optimistic-lock collision deterministic?

<details>
<summary>Answers</summary>

- **Mocks only replay your assumptions** (like mocking `fetch` in Jest: the test stays green when the real API changes). A mocked repository has no transactions, no `version` column, no primary key, so it would have hidden the double reservation (`@Version`), whether `processed_events` really shares the transaction, and the DLT name / double-encoding bugs. Mock your own collaborators in unit tests; use real infrastructure at the boundaries.
- **Proving "it did NOT happen" needs two steps:** `await().until(count == 1)` alone passes at the first effect, before the duplicate is even processed (green even with the bug). `Thread.sleep` + one assert checks a single moment and can pass for the wrong reason on a slow machine. So: wait for the first effect, then `during(3s)` = must *stay* true, checked every 100 ms, fails the moment it breaks.
- **`REQUIRES_NEW`** pauses the current transaction and runs a completely separate one (own connection, own entity copy), like a second browser tab, in the same thread. The interleaving A-read → B-write-commit → A-write is fixed in code, so the collision happens on every run; real threads collide only when the timing happens to line up.

</details>


## Phase 6: Break it on purpose

Full write-up with numbers: [docs/experiments.md](experiments.md). Summary of what each failure taught:

| Failure | Observed | Lesson |
|---|---|---|
| SIGKILL consumer during 300 orders | 1 redelivery, skipped as duplicate; 300/300 CONFIRMED; stock exactly −300; 27 s before partitions were reassigned | at-least-once is real; idempotency makes it safe; a crashed consumer holds its partitions until `session.timeout.ms` |
| 5 duplicates of one event | 5× `Duplicate event ... skipping`, no change | duplicates are normal, not exceptional |
| Poison message | same partition waited 8.2 s, other partition 0.6 s; DLT with exception headers | blocking retries = head-of-line blocking per **partition** (not per key) |
| Kafka down during POST | 4a buffered → delivered later; 4b `send()` blocked 60 s → HTTP 500 but order saved; 4b′ crash → order stuck forever | dual write: two systems, no shared transaction |

### 🔎 HOW IT WORKS: Rebalancing and `session.timeout.ms`
- What it is:      A **rebalance** is the consumer group re-dividing partitions among its members (someone joined, left, or died).
- What it does:    After SIGKILL the dead consumers stayed in the group; the restarted service got its partitions 27 s after starting (45 s after the kill).
- How it does it:  Consumers send heartbeats. A clean shutdown sends "leave group" → immediate rebalance. A crash sends nothing; the broker waits `session.timeout.ms` (45 s default) without heartbeats before evicting the member. Partitions of a dead member are not processed by anyone during that time.
- Why it matters:  That wait is the price of not evicting healthy-but-slow consumers. Separately, `max.poll.interval.ms` (5 min default) evicts a consumer whose *processing* takes too long between polls, a common cause of mysterious rebalance loops.
- Common pitfall:  Every rebalance can cause redeliveries (uncommitted offsets are re-read by the new owner), so idempotency also protects against routine deploys and scaling, not only crashes.

### 🔎 HOW IT WORKS: Producer buffering, `max.block.ms`, `delivery.timeout.ms`
- What it is:      `send()` doesn't send; it puts the record in an in-memory buffer that a background thread delivers.
- What it does:    4a: returned in milliseconds while Kafka was down, delivered 20 s later. 4b: blocked the HTTP thread 60 s (`Topic order.created not present in metadata after 60000 ms`).
- How it does it:  To choose a partition, the producer needs **metadata** (topics → partitions → leader brokers). Missing metadata → `send()` blocks up to `max.block.ms` (60 s). With metadata, the record waits in the buffer and is retried until `delivery.timeout.ms` (120 s), then the future fails.
- Why it matters:  The buffer hides short outages (good) but is volatile memory (a crash loses it), and `send()` can block the caller (an HTTP thread here). Our earlier assumption "send() never blocks" was wrong in 4b.
- Common pitfall:  Treating "`send()` returned" as "Kafka has it". Only the completed future (broker ack) means that.

### 🔎 HOW IT WORKS: Dual-write problem and transactional outbox
- What it is:      Writing to two systems (DB + broker) without a shared transaction: one can succeed and the other fail. The **outbox** fixes it by writing the event as a row in the same DB transaction, and publishing from that table afterwards.
- What it does:    Without outbox (observed): order committed, event lost → stuck forever; or HTTP 500 for an order that exists. With outbox: the order and its event are committed together or not at all.
- How it does it:  A relay (poller, or change-data-capture like Debezium reading the database's write-ahead log) reads unpublished outbox rows, publishes, marks them published. A crash between publish and mark → republish → at-least-once, absorbed by idempotent consumers.
- Why we use it:   It's the standard answer whenever a service must "update my DB and tell others". Alternatives: Kafka transactions (only atomic across Kafka writes, not with Postgres), 2PC/XA (Kafka doesn't participate), "listen to yourself" (publish first, update DB when consuming your own event: works but makes every read eventually consistent within one service).
- Common pitfall:  Publishing from the outbox without a stable `eventId` or ordering: always reuse the stored `eventId`, publish in insertion order per key.
- Status: explained, **not implemented** (brief: only if asked).

### Check your understanding
1. In experiment 1 exactly one event was redelivered after the kill. Why that one, and why did stock still end at exactly 700?
2. Why did `C2` wait 8.2 s although its key had nothing to do with the poison message? What would you change if that delay were unacceptable, and what would you give up?
3. In 4b the client got a 500 but the order existed. How would a transactional outbox change what the client sees, and what happens if the relay crashes right after publishing?

<details>
<summary>Answers</summary>

- **Exp 1 timing:** the kill landed between the DB commit (stock + `processed_events`, one transaction) and the offset commit. Kafka redelivered from the old bookmark; `processed_events` recognised the `eventId` → skipped → 300 reservations exactly once → 700.
- **Head-of-line blocking options:** shorter/fewer retries (less tolerance for transient errors), more partitions (lower *chance* of sharing a partition, not zero; more consumers needed), non-blocking retry topics (no wait, but **per-key ordering is lost**: a later event for the same order may overtake the retried one). Trade-off = latency vs ordering; we keep blocking retries because saga order matters.
- **4b was not a crash.** `save()` committed in its own transaction, then `send()` blocked 60 s and threw; Spring MVC turned the exception into HTTP 500. 4b′ was the crash: the client got *no response at all* (a 500 is something a running server sends).
- **Outbox removes Kafka from the request:** one DB transaction writes order + event row → `201` in ms even with Kafka down; a relay publishes later. Crash after commit → order *and* event survive. It does **not** guarantee the client hears back; a client retry after a lost response needs an idempotent POST (`Idempotency-Key`). Relay crash after publish but before marking → republish with the same `eventId` → consumers skip it.

</details>


## Phase 7: CI (GitHub Actions)

Goal: every push and pull request builds and tests all three services, including the Testcontainers tests, on a clean machine. The first real run happens when we push in Phase 9 (no remote yet); until then the workflow was checked locally: YAML parse, `actionlint` (0 errors), and the exact CI command `./mvnw -B verify` per service (20 tests, BUILD SUCCESS).

Versions checked via the GitHub API on 2026-10-13: `actions/checkout@v7`, `actions/setup-java@v6`, `actions/upload-artifact@v7`. Runner `ubuntu-24.04` (image 20261004) ships Docker Server 28.0.4, which Testcontainers needs.

```
push / pull_request
      │
      ▼
 workflow "CI" ── job "service" × matrix ──┬─ order-service         ┐ each on a fresh ubuntu-24.04 VM:
                                           ├─ inventory-service     │ checkout → setup Java 21 (+ Maven cache)
                                           └─ notification-service  ┘ → ./mvnw -B verify → (on failure) upload reports
```

### 🔎 HOW IT WORKS: Workflow, trigger, job, step, runner
- What it is:      A **workflow** is a YAML file in `.github/workflows/`. **Triggers** (`on:`) say when it runs. A workflow has **jobs**; each job runs on its own **runner** (a fresh virtual machine) and consists of **steps** executed in order.
- What it does:    `on: push` + `pull_request` → every commit to any branch and every PR runs the job three times (once per service), in parallel.
- How it does it:  GitHub starts a new VM per job from the `ubuntu-24.04` image, checks out the commit, runs the steps, and throws the VM away. Nothing carries over between runs except explicit caches/artifacts.
- Why we use it:   Proves the project builds and passes tests from a clean clone, not just on one Mac with a warm `~/.m2` and a running Compose stack (exactly the trap the old smoke tests fell into).
- Common pitfall:  `ubuntu-latest` moves to a new OS version on GitHub's schedule; a build can break without any change on our side. We pin `ubuntu-24.04` and upgrade deliberately. And a YAML 1.1 trap we saw locally: a generic parser reads a bare `on:` as the boolean `true`. GitHub's parser handles it, but generic tools may not.

### 🔎 HOW IT WORKS: Matrix strategy
- What it is:      One job definition expanded into several jobs, one per value (`service: [order-service, inventory-service, notification-service]`).
- What it does:    Three parallel jobs named after the service; `working-directory: services/${{ matrix.service }}` points each at its folder.
- How it does it:  `${{ matrix.service }}` is an **expression**, replaced per job before it runs.
- Why we use it:   No copy-pasted job per service; a fourth service is one more list entry. `fail-fast: false` keeps the other jobs running when one fails, so one push shows all failures at once (default `true` would cancel them).
- Common pitfall:  Matrix values used where they don't exist (typo `matrix.servce`): plain YAML is fine with it, the run fails late or silently produces wrong names. `actionlint` caught exactly that in our demo.

### 🔎 HOW IT WORKS: `actions/setup-java` + Maven cache
- What it is:      An official action that installs a JDK (Temurin 21) and can cache `~/.m2/repository`.
- What it does:    First run downloads all dependencies (~minutes); later runs restore them in seconds.
- How it does it:  `cache: maven` saves the Maven repository at the end of a job under a key derived from a hash of `cache-dependency-path` (each service's `pom.xml`). Same pom → same key → restored. Changed pom → new key → fresh download, then saved.
- Why we use it:   Faster feedback, less load on Maven Central. Alternative: `actions/cache` with a hand-written key: same mechanism, more lines.
- Common pitfall:  A cache key that never changes (stale dependencies forever) or always changes (cache never hits). Keying on the pom avoids both.

### 🔎 HOW IT WORKS: `permissions`, artifacts, timeouts
- `permissions: contents: read`: every run gets a temporary `GITHUB_TOKEN`; we limit it to reading the repo. If a dependency or action were compromised, it couldn't push code or create releases with our token (least privilege).
- `upload-artifact` with `if: failure()`: only when a step failed, the Surefire reports are attached to the run for download. Logs show *that* a test failed; reports show the full assertion message and stack trace.
- `timeout-minutes: 20`: a hung test (e.g. waiting on a container forever) fails the job instead of burning the default 6 hours of runner time.

### 🔎 HOW IT WORKS: `actionlint`
- What it is:      A static checker for GitHub Actions workflows (installed via Homebrew, 1.7.12).
- What it does:    Our workflow: 0 errors. Injected mistakes: `runs_on` → `unexpected key "runs_on"` + `"runs-on" section is missing`; `matrix.servce` → `property "servce" is not defined`.
- How it does it:  Knows the workflow schema, the types of every context (`matrix`, `github`, `secrets`...), and the inputs of popular actions.
- Why we use it:   The alternative feedback loop is push → wait for a runner → read the error → fix → push again.
- Common pitfall:  Linting proves the file is well-formed, not that the build works. That's what running `./mvnw -B verify` locally and the first real run are for.

### Why CI is the verification gate for agent-written code
An agent (like the one writing this project) can be confidently wrong: we've seen several "should work" moments in this log that failed when actually run (`422` vs `400`, the DLT name, `RestClient.Builder` missing, a stale-cache false alarm). CI makes "it works" a fact checked on a clean machine for every change, independent of who or what wrote the code and of what they *say* about it. Combined with branch protection (merge only when CI is green), nothing reaches `main` unverified.

### Not added (yet)
- The MCP server build job: added in Phase 8 together with the server, instead of a placeholder now.
- Docker image builds in CI: nothing consumes the images outside local Compose (no registry, no deployment; out of scope).

### Check your understanding
1. Why does every CI job start on a fresh VM, and which earlier problem in this project would that have caught immediately?
2. What does the Maven cache key depend on, and what happens on the next run after adding a dependency?
3. Why `permissions: contents: read`? What could go wrong without it?

<details>
<summary>Answers</summary>

- **What a fresh VM would have caught at once:** the old `contextLoads` smoke tests (Phases 1-4) passed only because the Compose stack was running on localhost. On a clean runner there is nothing on `localhost:5432/9092`, so they fail immediately. Fresh VMs expose hidden dependencies on local state (running containers, warm `~/.m2`, env vars).
- **Cache = speed only.** Key = hash of each service's `pom.xml`; a new dependency → miss → full download → new cache saved → later runs hit. A miss never breaks a correct build; a build that only works *because* of the cache is a bug.
- **Why `contents: read`:** the threat is foreign code running with the job's token: a compromised third-party action, a malicious dependency/plugin executing during the build, or a fork PR's code. With write access it could push to `main`, move tags, edit releases or workflows. Least privilege limits it to reading.

</details>


## Phase 8: MCP server (TypeScript)

Goal: let an AI agent inspect the running system through three read-only tools, registered in Claude Code.

Versions (checked 2026-10-13): **MCP TypeScript SDK v2** (`@modelcontextprotocol/server` / `client` 2.3.1). The SDK README calls v2 the stable line since 2026-07-27 (MCP spec 2026-07-28); v1 (`@modelcontextprotocol/sdk` 1.32) only gets fixes. Zod 4.6.7, TypeScript 7.0.2 (the native compiler), Vitest 5.0.4, Node 24.

```
Claude Code (host + MCP client) ──spawns──► node mcp-server/dist/index.js   (stdio: JSON-RPC on stdin/stdout)
        │  tools/list, tools/call                     │ zod schema check → handler → fetch
        ▼                                             ▼
   the model picks a tool                    order-service REST (GET /orders/..., read-only)
```

Tools: `get_order_status(orderId)`, `list_stuck_orders(olderThanMinutes = 5)`, `explain_order_timeline(orderId)`.
Live result (real data from Phase 6): `list_stuck_orders` → the 2 stranded orders; `explain_order_timeline(2ff5e3bf-...)` → "STUCK: PENDING for 98 minute(s) ... Missing step: an inventory.reserved or inventory.rejected event ... order.created eventId baca2786-..." + a 4-step checklist; `get_order_status("not-a-uuid")` → `Input validation error: ... orderId: Invalid UUID`.

### 🔎 HOW IT WORKS: MCP (Model Context Protocol)
- What it is:      An open protocol for connecting AI applications (**hosts** like Claude Code, OpenCode, Cursor) to external capabilities (**servers** like ours). Like a REST API contract, but designed for a model to discover and call.
- What it does:    Claude Code starts our server, asks `tools/list`, shows the tools to the model, and runs `tools/call` when the model decides to use one.
- How it does it:  JSON-RPC 2.0 messages. The host contains an MCP **client** (one per server). Session start: `initialize` (version + capability negotiation), then requests in both directions.
- Why we use it:   One server works with every MCP host and every model behind it; no per-app plugin code.
- Common pitfall:  Treating it as "an API for the model". The model only sees names, descriptions and schemas, and decides from those alone. Vague descriptions = wrong or missing tool calls.

### 🔎 HOW IT WORKS: Tools vs resources vs prompts
- **Tools**: actions the *model* decides to call, with arguments (`explain_order_timeline`). Our three are all tools because the model needs to choose which order to look at.
- **Resources**: read-only data the *application/user* attaches as context, addressed by URI (e.g. `order://{id}` or a log file). Pull-based, no "decision" by the model.
- **Prompts**: reusable prompt templates the *user* picks (e.g. a slash command "/investigate-order {id}").
- Rule of thumb: model-driven → tool; user/app-driven context → resource; user-driven workflow → prompt.

### 🔎 HOW IT WORKS: stdio transport
- What it is:      The host launches the server as a child process and talks over its stdin/stdout.
- What it does:    `serveStdio(createServer)` reads JSON-RPC from stdin, writes to stdout. Our banner `order-platform MCP server on stdio, ...` goes to stderr.
- How it does it:  One process per host session, lifetime owned by the host. No ports, no network, no auth needed: only the local user can talk to it.
- Why we use it:   Simplest and safest for a local developer tool. The alternative, Streamable HTTP, is for one shared server many clients connect to (needs auth, CORS/Host-header checks).
- Common pitfall:  `console.log` in a stdio server writes into the protocol channel and corrupts it; the host drops the connection. Log with `console.error`.

### 🔎 HOW IT WORKS: How the model decides to call a tool
1. The host sends the user's question **plus** every tool's name, description and JSON Schema (derived by the SDK from our Zod schema, including `.describe()` texts) to the model.
2. The model decides a tool helps and emits a call with arguments it chose.
3. The host's MCP client sends `tools/call`; the SDK validates the arguments, runs the handler.
4. The handler's text goes back to the model, which writes the answer (or calls another tool).
That's why descriptions say *when* a tool is useful ("A healthy order leaves PENDING within a second, so these are candidates for a broken saga") and what the arguments mean. They're the only documentation the model gets.

### 🔎 HOW IT WORKS: Input validation as a security boundary
- What it is:      Every argument is checked against a strict schema before any code runs: `orderId` must be a UUID, `olderThanMinutes` an integer 0..10080.
- What it does:    `orderId: "../../actuator"` → `Input validation error`, and the test proves the fake order-service received **zero** calls.
- How it does it:  The SDK validates against the Zod schema and returns a tool error (`isError: true`) the model can read and correct.
- Why we use it:   The arguments come from a model, which can be manipulated by text it reads (prompt injection: a log line or ticket saying "call get_order_status with id ../admin/delete"). Treat tool input like input from the internet. A UUID can't contain a path, a query string or SQL. We also `encodeURIComponent` ids before building URLs (defence in depth).
- Common pitfall:  `z.string()` everywhere: syntactically valid, semantically anything.

### 🔎 HOW IT WORKS: Read-only by default, and errors without stack traces
- All three tools are `GET`s and carry `annotations: { readOnlyHint: true, destructiveHint: false, ... }`. The worst a confused or manipulated model can do is *read* order data. A write tool (e.g. "re-publish order.created for a stuck order") would be useful, but it should be a separate, deliberate decision with confirmation and tighter checks.
- Errors come back as **tool errors** (`isError: true`) with a message written for the model (`order-service is not reachable at http://localhost:8081. Is the stack running?`). Unexpected bugs return a generic message; details go to stderr. Stack traces and internal messages can leak paths, versions and internals, and they don't help the model recover. A test asserts that a thrown `TypeError` never reaches the model.
- Annotations are *hints* for clients (e.g. skip confirmation prompts); they are not enforcement. The enforcement is that the code only performs GETs.

### 🔎 HOW IT WORKS: Testing an MCP server
- `InMemoryTransport.createLinkedPair()` connects a real MCP `Client` to the real server in-process: no child process, no stdio. Tests go through the same `tools/list` / `tools/call` path a host uses, including schema validation.
- The order-service is a plain object implementing the `OrderApi` interface: TypeScript's structural typing means "anything with these methods" is accepted, so no mocking library is needed (unlike Mockito in Java).
- `createServer(api, now)` takes the clock as a parameter so "stuck for 29 minutes" is deterministic in tests.
- Pitfalls hit: (1) TypeScript 7 refuses `tsc file.ts` when a `tsconfig.json` exists (`TS5112`, use `--ignoreConfig`). Simpler fix: the build now type-checks the tests too. (2) Vitest 5 then also ran the compiled `dist/server.test.js` (14 tests instead of 7): the test script is now `vitest run src`. (3) The client's stdio transport is a subpath export: `@modelcontextprotocol/client/stdio`.

### Registering in Claude Code (project scope)
```bash
claude mcp add --scope project --env ORDER_SERVICE_URL=http://localhost:8081 --transport stdio order-platform -- node mcp-server/dist/index.js
```
- Writes `.mcp.json` in the repo root (committed, shared with anyone who clones). `local` scope would store it privately in `~/.claude.json` instead; `user` scope = all your projects.
- Pitfall hit: `--env` takes several `KEY=value` pairs, so a server name directly after it is read as another pair (`Invalid environment variable format: order-platform`). Put another option between them.
- Project servers start as `⏸ Pending approval`: a cloned repo can't approve its own servers. You approve once in an interactive `claude` session.
- `dist/` is not committed: run `npm ci && npm run build` in `mcp-server/` first.
- `claude -p` (headless mode) loads project servers without the approval prompt, useful for scripted checks; restrict it with `--allowedTools`.

### Other clients (e.g. OpenCode with any LLM)
The server is identical; only the host config differs. OpenCode: `opencode.json` → `"mcp": { "order-platform": { "type": "local", "command": ["node", "mcp-server/dist/index.js"], "environment": { "ORDER_SERVICE_URL": "http://localhost:8081" } } }`. Cursor: `.cursor/mcp.json` with `mcpServers` + `command`/`args`. What changes with a different model is *how well it uses* the tools, which is why clear descriptions and server-side validation matter.

### Limitation
The tools only see order-service's REST API. `explain_order_timeline` can say *what to check* for a stuck order but can't check Kafka itself (we know from Phase 6 that `baca2786-...` never reached the topic; the tool can't). Read-only Kafka tools (consumer lag, find event by key) would close that gap, outside this brief's architecture.

### Check your understanding
1. What is the difference between an MCP **tool** and a **resource**? Why are ours tools?
2. A model can be manipulated by text it reads (prompt injection). What limits the damage, even if it calls our tools with strange arguments?
3. Why must a stdio MCP server never use `console.log`? Where do its logs go, and who sees them?

<details>
<summary>Answers</summary>

- **Tool vs resource = who decides.** Tools are model-controlled (the model chooses whether and with which arguments); resources are application/user-controlled context attached by URI. Ours are tools because the model must choose which order and when. Also keeps context small: data is fetched only when useful.
- **Two layers against a manipulated model:** (1) the Zod schema is the enforced boundary (descriptions only guide a well-behaved model); (2) the tools are read-only, with no write capability at all, so even perfect malicious arguments can only read. Plus: sanitized errors keep internals out of the model's context.
- **Who runs what:** the host (Claude Code) spawns the server and its MCP client sends `tools/call`; the model only requests calls. stdout is the JSON-RPC wire, so a `console.log` corrupts it and usually drops the connection. stderr is captured by the host for the developer; the model never sees it.

</details>


## Phase 8b: read-only Kafka tool (`find_order_events`)

Added after the first real conversation raised two questions: "the agent offered to remove the stuck orders; could it delete them?" and "can the MCP server read Kafka too?"

- **An agent's power = the union of all its host's tools.** Our MCP server is read-only, but Claude Code also has Bash: it could run `psql ... DELETE` if the human approves the prompt. Protection lives around the agent: permission prompts, restricted tool sets (`--allowedTools`), read-only DB credentials. Deleting the rows would also be the wrong fix (it hides a lost event).
- **Client library:** `kafkajs` (most downloads) has had no release since 2023-02, a risk against Kafka 4.x, which removed old protocol versions. `@confluentinc/kafka-javascript` is official but a native librdkafka binding. Chose `@platformatic/kafka` 2.14.0: pure TypeScript, actively released.
- **How it stays read-only:** a throwaway consumer group (`mcp-inspect-<uuid>`) reading from EARLIEST with `autocommit: false`, stopping once every partition reaches its end offset (`listOffsets` LATEST), 10 s cap, group deleted afterwards (verified: 0 leftover groups). Lag = end offset − committed offset per service group (`listConsumerGroupOffsets`).
- **Live result:** stuck order `2ff5e3bf...` → "No order.created event ... it was never published. This is the dual-write gap", exactly what Phase 6 experiment 4b showed by hand. A confirmed order → `order.created` → `inventory.reserved` → `order.confirmed` (325 ms) and all lag 0. ~3 s per call (joining the group).
- **Bugs found by running it live:** (1) an id that doesn't exist got the "never published" verdict: Kafka alone can't tell "never published" from "never existed", so the tool now asks order-service first; (2) events came out grouped by topic, now sorted by timestamp.
- **ponytail:** scans whole topics per call; with real volume search by timestamp (`listOffsets` with the order's `createdAt`) or keep an index.
