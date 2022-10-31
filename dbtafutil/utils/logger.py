import logging
from datetime import datetime
from colorama import init, Fore, Back
import os

init(autoreset=True)
LOGGER_ROOT_NAME = 'dbtafutil'

class CustomFormatter(logging.Formatter):

    formatDebug = "%(asctime)s [%(levelname)s]: %(name)s - %(message)s (%(filename)s:%(lineno)d)"
    formatInfo = "%(asctime)s [%(levelname)s]: %(message)s"
    formatError = "%(asctime)s [%(levelname)s]: %(message)s (%(filename)s:%(lineno)d)"

    FORMATS = {
        logging.DEBUG: Fore.WHITE + formatDebug + Fore.RESET,
        logging.INFO: Fore.GREEN + formatInfo + Fore.RESET,
        logging.WARNING: Fore.YELLOW + formatError + Fore.RESET,
        logging.ERROR: Fore.RED + formatError + Fore.RESET,
        logging.CRITICAL: Fore.WHITE + Back.RED + formatError + Fore.RESET + Back.RESET
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)

class Logger():
    def initialize(self, env='dev'):
        logger = logging.getLogger(LOGGER_ROOT_NAME)
        console = logging.StreamHandler()
        if env.lower() == 'dev':
            logger.setLevel(logging.DEBUG)
            console.setLevel(logging.DEBUG)
        elif env.lower() == 'prod':
            logger.setLevel(logging.ERROR)
            console.setLevel(logging.ERROR)
        else:
            logger.setLevel(logging.INFO)
            console.setLevel(logging.INFO)
        
        console.setFormatter(CustomFormatter())
        logger.addHandler(console)

        # print(os.path.dirname(os.path.abspath(__file__)))
             
        return logger

    def getRootLoggerName() -> str:
        return LOGGER_ROOT_NAME

def main():
    logger = Logger().initialize()
    logger.info("This is an info message")
    logger.warning("This is a warning message")
    logger.debug("This is a debug message")
    logger.error("This is an error message")
    logger.critical("This is a critical message")

if __name__ == "__main__":
    main()