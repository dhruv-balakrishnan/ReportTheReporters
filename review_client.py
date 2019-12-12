from modules import html_processor, html_worker
import logging
import os
import argparse
"""
Entrypoint.
"""

_SCRIPT_DIR = os.path.realpath(os.path.dirname(__file__))
__WORKDIR__ = os.path.abspath(_SCRIPT_DIR)


def run(logger, page_count):
    logger.info("Setting up..")
    data_directory = os.path.join(__WORKDIR__, "data")
    input_data_directory = os.path.join(data_directory, "clean")
    output_data_directory = os.path.join(__WORKDIR__, "output")

    logger.info("Grabbing Data..")
    # The default number of files to work on is 5.
    html_worker.GetData(page_count, data_directory, True)

    logger.info("Processing Data..")
    # CALL HERE
    html_processor.main(input_data_directory)


def init():
    logging.captureWarnings(True)
    logger = logging.getLogger()
    logger.setLevel(logging.getLevelName("INFO"))

    handler = logging.StreamHandler()

    log_dir = os.path.join(__WORKDIR__, "logs")
    clean_data_dir = os.path.join(__WORKDIR__, "data", "clean")

    if not os.path.exists(log_dir):
        os.mkdir(log_dir)

    if not os.path.exists(clean_data_dir):
        os.mkdir(clean_data_dir)

    file_handler = logging.FileHandler(os.path.join(log_dir, "log.txt"))

    formatter = logging.Formatter(
        '%(asctime)s %(name)-20s %(levelname)-8s %(message)s {}'.format(""))

    handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(handler)

    return logger


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Run ReportTheReporters')
    parser.add_argument('--page_count', dest='page_count', type=int, default=100,
                        help='How many HTML pages to process. Leave empty for all. Default is 5.')

    args = parser.parse_args()

    logger = init()
    run(logger, args.page_count)

