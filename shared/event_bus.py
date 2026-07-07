from __future__ import annotations

import asyncio
import json
import logging
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable

from shared.events import BaseEvent, EVENT_REGISTRY, Topic

logger = logging.getLogger("event_bus")

Handler = Callable[[BaseEvent], Awaitable[None]]


class EventBus(ABC):
    """
    Every service talks to the event bus through this interface only -
    it never imports aiokafka directly. That's what lets tests run against
    an in-memory fake instead of spinning up a real broker, the same trick
    used for the LLM provider gateway in the other project.
    """

    @abstractmethod
    async def publish(self, topic: Topic, event: BaseEvent) -> None: ...

    @abstractmethod
    def subscribe(self, topic: Topic, handler: Handler) -> None:
        """Register a handler to run whenever an event arrives on `topic`."""

    @abstractmethod
    async def start(self) -> None: ...

    @abstractmethod
    async def stop(self) -> None: ...


class KafkaEventBus(EventBus):
    """Real Kafka-backed implementation using aiokafka."""

    def __init__(self, bootstrap_servers: str, group_id: str) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._group_id = group_id
        self._producer = None
        self._consumer = None
        self._handlers: dict[Topic, list[Handler]] = {}
        self._consume_task: asyncio.Task | None = None

    def subscribe(self, topic: Topic, handler: Handler) -> None:
        self._handlers.setdefault(topic, []).append(handler)

    async def start(self) -> None:
        from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        await self._producer.start()

        if self._handlers:
            self._consumer = AIOKafkaConsumer(
                *[t.value for t in self._handlers],
                bootstrap_servers=self._bootstrap_servers,
                group_id=self._group_id,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="earliest",
            )
            await self._consumer.start()
            self._consume_task = asyncio.create_task(self._consume_loop())

    async def _consume_loop(self) -> None:
        assert self._consumer is not None
        try:
            async for msg in self._consumer:
                topic = Topic(msg.topic)
                event_cls = EVENT_REGISTRY[topic]
                event = event_cls.model_validate(msg.value)
                for handler in self._handlers.get(topic, []):
                    try:
                        await handler(event)
                    except Exception:
                        logger.exception("Handler failed for topic=%s event_id=%s", topic, event.event_id)
        except asyncio.CancelledError:
            pass

    async def publish(self, topic: Topic, event: BaseEvent) -> None:
        assert self._producer is not None
        await self._producer.send_and_wait(topic.value, event.model_dump(mode="json"))
        logger.info("Published event topic=%s event_id=%s order_id=%s", topic, event.event_id, event.order_id)

    async def stop(self) -> None:
        if self._consume_task:
            self._consume_task.cancel()
        if self._consumer:
            await self._consumer.stop()
        if self._producer:
            await self._producer.stop()


class InMemoryEventBus(EventBus):
    """
    Fake event bus for tests and local development without a Kafka broker.
    Dispatches published events straight to registered handlers on the
    same event loop. Lets the full saga (order -> inventory -> payment ->
    notification) run and be asserted on on in a unit test in milliseconds.
    """

    def __init__(self) -> None:
        self._handlers: dict[Topic, list[Handler]] = {}
        self.published: list[tuple[Topic, BaseEvent]] = []

    def subscribe(self, topic: Topic, handler: Handler) -> None:
        self._handlers.setdefault(topic, []).append(handler)

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def publish(self, topic: Topic, event: BaseEvent) -> None:
        self.published.append((topic, event))
        for handler in self._handlers.get(topic, []):
            await handler(event)
