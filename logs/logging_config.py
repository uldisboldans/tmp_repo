import logging
import os

def create_logger(name,log_file_name,project_directory_path,logging_level=logging.INFO):
    """Configure a logger object and return it.

    Arguments:
    name -- name of the logger, logged to the log file
    log_file_name -- name of the log file
    project_directory_path -- root directory
    logging_level -- ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL') default is logging.INFO 
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging_level) # Logging Levels (https://python.readthedocs.io/en/latest/library/logging.html)
    formatter = logging.Formatter("%(asctime)s : %(levelname)s : %(name)s : %(message)s") # LogRecord attributes (https://python.readthedocs.io/en/latest/library/logging.html)
    log_file_name = f"{log_file_name}.log" 
    log_file_path = os.path.join(project_directory_path,"logs",log_file_name) 
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger


