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
    else:
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
    logger.info("Done")


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
    r1 = test.rec(1, "test", 2, val2 = 3)
    r2 = test.rec(1, "test2", 2, val2 = 4)
    r3 = test.rec(1, "test2", val1 = 2, val2 = 4)
    print(r1.get())
    print(r1.get())
    print(r2.get())
    print(r2.get())
    print(r3.get())
    logger.info("Running end")