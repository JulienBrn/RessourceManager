import logging, beautifullogger
import sys

logger = logging.getLogger(__name__)

def setup_nice_logging():
    beautifullogger.setup(logmode="w")
    logging.getLogger("toolbox.ressource_manager").setLevel(logging.WARNING)
    logging.getLogger("toolbox.signal_analysis_toolbox").setLevel(logging.WARNING)

def handle_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        logger.info("Keyboard interupt")
        sys.exit()
        return
    else:
        sys.__excepthook__(exc_type, exc_value, exc_traceback)


sys.excepthook = handle_exception



from RessourceManager.ressource_manager import RessourceManager
rm = RessourceManager()
import time

@rm.ressource()
def test(wait, msg, val1, val2):
    print(msg)
    time.sleep(wait)
    return val1 + val2

def run():
    setup_nice_logging()
    logger.info("Running start")
    r = test.rec(1, "test", 2, val2 = 3)
    print(r.compute())
    logger.info("Running end")