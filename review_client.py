from modules import data_processor, html_worker
import logging
import os
"""
Sets up the program. Entrypoint.
"""
__FILEPATH__ = os.path.abspath(__file__)
__WORKDIR__ = os.path.dirname(__FILEPATH__)

def run(logger):
    logger.info("Starting Pipeline")
    data_directory = os.path.join(__WORKDIR__, "data")
    input_data_directory = os.path.join(data_directory, "clean")
    output_data_directory = os.path.join(__WORKDIR__, "output")

    logger.info("Organizing Environment..")
    html_getter = html_worker.html_worker(5, data_directory, True)
    logger.info("Processing Data..")
    html_processor = data_processor.spark_processor("Test", input_data_directory)


def init():
    logging.captureWarnings(True)
    logger = logging.getLogger()
    logger.setLevel(logging.getLevelName("INFO"))

    handler = logging.StreamHandler()
    file_handler = logging.FileHandler(os.path.join(__WORKDIR__, "logs", "log.txt"))

    formatter = logging.Formatter(
        '%(asctime)s %(name)-20s %(levelname)-8s %(message)s {}'.format(""))

    handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(handler)

    return logger

if __name__ == "__main__":
    logger = init()
    run(logger)

