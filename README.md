# Apache Kafka Basics

## What is Apache Kafka?

Apache Kafka is a **distributed event streaming platform** used to publish, store, and process streams of events.

A simple Kafka architecture:

```text
Producer
   |
   v
 Topic
   |
   v
Consumer
```

---

## 1. Producer

A **producer** is an application that sends messages (events) to Kafka.

```text
Producer → Kafka
```

Example:

```json
{
  "orderId": 101,
  "amount": 499
}
```

---

## 2. Consumer

A **consumer** is an application that reads messages from Kafka.

```text
Kafka → Consumer
```

Consumers can process events and perform actions based on them.

---

## 3. Topic

A **topic** is a named stream where Kafka stores messages.

Examples:

```text
orders
payments
users
notifications
```

Producers publish messages to topics, and consumers read messages from topics.

---

## 4. Message / Event

A **message** is the data stored in a Kafka topic.

Example:

```json
{
  "orderId": 101,
  "item": "Pizza",
  "amount": 499
}
```

---

## 5. Broker

A **broker** is a Kafka server responsible for storing and serving messages.

A Kafka cluster can contain multiple brokers:

```text
Kafka Cluster

+---------+    +---------+    +---------+
| Broker 1|    | Broker 2|    | Broker 3|
+---------+    +---------+    +---------+
```

---

## 6. Partition

A topic can be divided into multiple **partitions**.

```text
orders

Partition 0
Partition 1
Partition 2
```

Partitions allow Kafka to process messages in parallel and scale across multiple brokers and consumers.

---

## 7. Offset

Every message within a partition has an **offset**.

```text
Partition 0

Offset 0 → Event A
Offset 1 → Event B
Offset 2 → Event C
Offset 3 → Event D
```

The offset represents the position of a message within a partition.

---

## 8. Consumer Group

A **consumer group** is a group of consumers working together to consume messages from Kafka.

```text
             orders
                |
       +--------+--------+
       |        |        |
    Consumer Consumer Consumer
       1        2        3
```

Kafka distributes partitions among consumers in the same group.

This allows message processing to be parallelized.

---

## 9. Replication

Kafka can maintain multiple copies of partitions.

```text
Partition
   |
   +---- Broker 1
   |
   +---- Broker 2
   |
   +---- Broker 3
```

Replication helps protect data against broker failures.

---

## 10. Retention

Kafka can retain messages for a configured period of time.

For example:

```text
7 days
30 days
```

A message can remain in Kafka even after a consumer has read it.

---

## Basic Kafka Flow

```text
                 Kafka Cluster
                      |
              +-------+-------+
              |               |
           Topic           Topic
              |
         Partitions
              |
         Consumer Group
              |
        +-----+-----+
        |           |
    Consumer     Consumer
```

### In simple terms:

```text
Producer
   ↓
Topic
   ↓
Partition
   ↓
Consumer Group
   ↓
Consumer
```

These concepts form the foundation for understanding Apache Kafka.
