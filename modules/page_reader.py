from bs4 import BeautifulSoup as soup
import logging
import json
import sys
import requests
import time
import os

sys.path.append("..\\")


def read_page(data):
    """
    Read a pages HTML content and extract relevant data including:
    - author
    - keywords
    - title
    - news location
    - paragraph text (all)
    :param data: the location of the text file
    :return: nothing
    """
    with open(data, 'r') as f:
        text = f.read()
        page = soup(text, 'html.parser')

        author = page.find("meta", {"name":"author"})["content"]
        keywords = page.find("meta", {"name":"keywords"})["content"].split(',')
        title_arr = page.title.text.split('|')
        title = title_arr[0]
        news_location = title_arr[1]

        print(f"{title}, {news_location}\n{author}\n{keywords}")


def get_pages_from_url(url):
    """
    Get data from a page URL, and save it as a text file for later processing.
    :param url: the page URL
    :return: the path to the file
    """
    file_path = None
    try:
        time.sleep(1) # To mitigate server load
        page = requests.get(url)

        if page.status_code == 200:
            file_name = url.split('/')[-1].split('?')[0]
            file_path = f"..\\data\\clean\\{file_name}.txt"

            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(page.text)

            logger.debug(f"  File-Path: {file_path}")
        else:
            logger.warning(f"Error response when fetching {url}: {page.status_code}")

    except ConnectionError as ce:
        logger.error(f"Error fetching data from {url}: {ce}")

    except IOError as ioe:
        logger.error(f"Error writing to file: {ioe}")

    except UnicodeEncodeError as uee:
        logger.error(f"Error writing to file because of format: {uee}")

        # File could be removed in-between check and delete
        if os.path.isfile(file_path):
            os.remove(file_path)

    return file_path


def prepare_clean_data(input):
    """
    Reads the url_list.txt file, captures the url for a page, and calls other
    methods to save clean data for later processing.
    :param input: path to the input text file containing the URLs
    :return: None
    """
    logger.info("Preparing Data.")

    with open(input, 'r') as url_list:
        for index, line in enumerate(url_list):
            url_obj = json.loads(line)
            get_pages_from_url(url_obj["url"])
            if index % 10 == 0:
                logger.info(f"  Processed {index+1} files")


if __name__ == "__main__":

    logging.captureWarnings(True)
    logger = logging.getLogger()
    logger.setLevel(logging.getLevelName("INFO"))

    handler = logging.StreamHandler()
    file_handler = logging.FileHandler("{}/{}".format("..\\output\\", "log.txt"))

    formatter = logging.Formatter(
        '%(asctime)s %(name)-20s %(levelname)-8s %(message)s {}'.format(""))

    handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(handler)

    prepare_clean_data("..\\data\\url_list.txt")
