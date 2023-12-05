import logging
import logging.config
import os
import sys
from datetime import datetime, timezone
import zipfile

from src.config import read_config_file

config = read_config_file()

current_file = os.path.abspath(__file__)
current_directory = os.path.dirname(current_file)

# create a logger class that uses message as input
class Log:
    def __init__(self):
        logging.config.fileConfig(current_directory + "/log.conf")
        self.logger = logging.getLogger("sLogger")
        # self.logger.setLevel(logging.DEBUG)
        # loggingStreamHandler = logging.StreamHandler()
        # if not self.logger.handlers:
        #     self.logger.addHandler(loggingStreamHandler)

    def debug(self, message):
        self.logger.debug(str(message))

    def info(self, message):
        self.logger.info(str(message))

    def warning(self, message):
        self.logger.warning(str(message))

    def error(self, message):
        self.logger.error(str(message))

    def critical(self, message):
        self.logger.critical(str(message))

    def zip_and_delete(self):
        os.chdir('/data/log/')
        file_size = os.path.getsize('output.log')
        
        if file_size > (config['logging']['max_size'] * 1000):
            zip_filename = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H-%M-%S.zip")
            with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_LZMA) as zipf:
                zipf.write('output.log')
            
            os.remove('output.log')

        zip_files = os.listdir(os.getcwd())
        for zip_file in zip_files:
            if zip_file[-3:] == "zip":
                current_date = datetime.now(timezone.utc)
                creation_date = datetime.strptime(zip_file, "%Y-%m-%d-%H-%M-%S.zip").replace(tzinfo=timezone.utc)

                if (current_date - creation_date).days > 30:
                    os.remove(zip_file)


# class Log:
#     def __init__(self):
#         self.logger = logging.getLogger("ftx_rates_service")
#         self.logger.setLevel(logging.DEBUG)
#         loggingStreamHandler = logging.StreamHandler()
#         if not self.logger.handlers:
#             self.logger.addHandler(loggingStreamHandler)

#     def debug(self, message):
#         self.logger.debug(
#             {
#                 "severity": "debug",
#                 "message": str(message),
#                 "timestamp": datetime.utcnow().strftime("%B %d %Y - %H:%M:%S"),
#             },
#             extra={"labels": {"app": "ftx_rates_service"}},
#         )

#     def info(self, message):
#         self.logger.info(
#             {
#                 "severity": "info",
#                 "message": str(message),
#                 "timestamp": datetime.utcnow().strftime("%B %d %Y - %H:%M:%S"),
#             },
#             extra={"labels": {"app": "ftx_rates_service"}},
#         )

#     def warning(self, message):
#         self.logger.warning(
#             {
#                 "severity": "warning",
#                 "message": str(message),
#                 "timestamp": datetime.utcnow().strftime("%B %d %Y - %H:%M:%S"),
#             },
#             extra={"labels": {"app": "ftx_rates_service"}},
#         )

#     def error(self, message):
#         self.logger.error(
#             {
#                 "severity": "error",
#                 "message": str(message),
#                 "timestamp": datetime.utcnow().strftime("%B %d %Y - %H:%M:%S"),
#             }
#         )

#     def critical(self, message):
#         self.logger.critical(
#             {
#                 "severity": "critical",
#                 "message": str(message),
#                 "timestamp": datetime.utcnow().strftime("%B %d %Y - %H:%M:%S"),
#             }
#         )