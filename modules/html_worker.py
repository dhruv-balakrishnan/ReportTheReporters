from bs4 import BeautifulSoup as soup
import logging
import json
import sys
import requests
import time
import os

_SCRIPT_DIR = os.path.realpath(os.path.dirname(__file__))
__WORKDIR__ = os.path.abspath(os.path.join(_SCRIPT_DIR, '..'))


class GetData:


    def _read_page(self, data):
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

            author = page.find("meta", {"name": "author"})["content"]
            keywords = page.find("meta", {"name": "keywords"})["content"].split(',')
            title_arr = page.title.text.split('|')
            title = title_arr[0]
            news_location = title_arr[1]

            print(f"{title}, {news_location}\n{author}\n{keywords}")


    def _get_pages_from_url(self, url):
        """
        Get data from a page URL, and save it as a text file for later processing.
        :param url: the page URL
        :return: the path to the file
        """
        file_path = None
        try:
            time.sleep(1)  # To mitigate server load
            page = requests.get(url)

            if page.status_code == 200:
                file_name = url.split('/')[-1].split('?')[0] + ".txt"

                # TODO: Refactor this to use a variable instead of a hardcoded string path
                file_path = os.path.join(self.clean_data_directory, file_name)

                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(page.text)

                self.logger.debug(f"  File-Path: {file_path}")
            else:
                self.logger.warning(f"Error response when fetching {url}: {page.status_code}")

        except ConnectionError as ce:
            self.logger.error(f"Error fetching data from {url}: {ce}")

        except IOError as ioe:
            self.logger.error(f"Error writing to file: {ioe}")

        except UnicodeEncodeError as uee:
            self.logger.error(f"Error writing to file because of format: {uee}")

            # File could be removed in-between check and delete
            if os.path.isfile(file_path):
                os.remove(file_path)

        return file_path


    def prepare_clean_data(self, input, page_limit):
        """
        Reads the url_list.txt file, captures the url for a page, and calls other
        methods to save clean data for later processing.
        :param input: path to the input text file containing the URLs
        :return: None
        """
        self.logger.info("Preparing Data.")

        with open(input, 'r') as url_list:
            for index, line in enumerate(url_list):
                url_obj = json.loads(line)
                self._get_pages_from_url(url_obj["url"])

                if index % 100 == 0:
                    self.logger.info(f"  Processed {index + 1} files")

                if page_limit is not None and index == page_limit:
                    self.logger.info("  Page Limit reached.")
                    break


    def prepare_environment(self):
        """
        Prepares the environment for use.
        :return: None
        """
        """
            os.remove() removes a file.
            os.rmdir() removes an empty directory.
            shutil.rmtree() deletes a directory and all its contents.
        """
        self.logger.info("Cleaning data directory.")
        for file in os.listdir(self.clean_data_directory):
            if os.path.exists(os.path.abspath(file)):
                os.remove(os.path.abspath(file))


    def __init__(self, page_limit, data_directory, clean_env=True):
        """
        Initialization. The page_limit variable specifies how many pages to download from the url input files
        :param page_limit: the number of pages to download/process
        :param clean_env: whether or not to clean the environment before use.
        """
        self.logger = logging.getLogger()
        self.data_directory = data_directory
        self.clean_data_directory = os.path.join(data_directory, "clean")

        if clean_env:
            self.prepare_environment()

        # TODO: Refactor this to put everything in a config file
        input_urls_loc = os.path.join(self.data_directory, "url_list.txt")
        self.prepare_clean_data(input_urls_loc, page_limit)
