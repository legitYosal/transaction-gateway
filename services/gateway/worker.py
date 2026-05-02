import logging
import signal
import threading

from common.logging import configure_logging

from .clients import LedgerClient
from .services.outbox import TransactionLedgerOutboxService
from .settings import get_settings


settings = get_settings()

configure_logging(settings.logging_level.upper())

logger = logging.getLogger(__name__)

POLL_INTERVAL_SECONDS = 2
shutdown_event = threading.Event()


def request_shutdown(signum, frame) -> None:
    logger.info("shutdown signal received signal=%s", signum)
    shutdown_event.set()


def run_forever() -> None:
    signal.signal(signal.SIGINT, request_shutdown)
    signal.signal(signal.SIGTERM, request_shutdown)

    ledger_client = LedgerClient(settings.ledger_service_url)

    outbox_service = TransactionLedgerOutboxService(
        ledger_client=ledger_client,
    )

    logger.info("transaction ledger outbox worker started")

    while not shutdown_event.is_set():
        did_work = outbox_service.process_once()

        if not did_work:
            shutdown_event.wait(POLL_INTERVAL_SECONDS)

    logger.info("transaction ledger outbox worker stopped gracefully")


if __name__ == "__main__":
    run_forever()
