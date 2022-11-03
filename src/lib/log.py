import logging
from datetime import datetime


# create a logger class that uses message as input
class Log:
    def __init__(self):
        self.logger = logging.getLogger("ftx_rates_service")
        self.logger.setLevel(logging.DEBUG)
        loggingStreamHandler = logging.StreamHandler()
        if not self.logger.handlers:
            self.logger.addHandler(loggingStreamHandler)

    def debug(self, message):
        self.logger.debug(
            {
                "severity": "debug",
                "message": str(message),
                "timestamp": datetime.utcnow().strftime("%B %d %Y - %H:%M:%S"),
            },
            extra={"labels": {"app": "ftx_rates_service"}},
        )

    def info(self, message):
        self.logger.info(
            {
                "severity": "info",
                "message": str(message),
                "timestamp": datetime.utcnow().strftime("%B %d %Y - %H:%M:%S"),
            },
            extra={"labels": {"app": "ftx_rates_service"}},
        )

    def warning(self, message):
        self.logger.warning(
            {
                "severity": "warning",
                "message": str(message),
                "timestamp": datetime.utcnow().strftime("%B %d %Y - %H:%M:%S"),
            },
            extra={"labels": {"app": "ftx_rates_service"}},
        )

    def error(self, message):
        self.logger.error(
            {
                "severity": "error",
                "message": str(message),
                "timestamp": datetime.utcnow().strftime("%B %d %Y - %H:%M:%S"),
            }
        )

    def critical(self, message):
        self.logger.critical(
            {
                "severity": "critical",
                "message": str(message),
                "timestamp": datetime.utcnow().strftime("%B %d %Y - %H:%M:%S"),
            }
        )
