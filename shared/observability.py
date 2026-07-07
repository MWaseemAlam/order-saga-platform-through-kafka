from __future__ import annotations

import logging
import sys

from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator


def configure_logging(service_name: str) -> logging.Logger:
    """
    Structured-ish logging: every line is prefixed with the service name so
    that when all four services' logs are aggregated (e.g. in `docker
    compose logs`, or shipped to Loki/ELK in production), you can filter by
    service and still see correlation via order_id in the message itself.
    """
    logging.basicConfig(
        level=logging.INFO,
        format=f"%(asctime)s | {service_name} | %(levelname)s | %(name)s | %(message)s",
        stream=sys.stdout,
    )
    return logging.getLogger(service_name)


def instrument_app(app: FastAPI, service_name: str) -> None:
    """
    Exposes a /metrics endpoint with request count, latency histograms, and
    in-progress request gauges out of the box - Prometheus can scrape this
    directly, no custom metric code needed per route.
    """
    Instrumentator().instrument(app).expose(app, endpoint="/metrics")
