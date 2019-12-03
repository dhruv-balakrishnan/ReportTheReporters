from pyspark import SparkContext, SparkConf
from pyspark.sql import *
from bs4 import BeautifulSoup as soup
import os
import string
import logging
import pandas

_SCRIPT_DIR = os.path.realpath(os.path.dirname(__file__))
__WORKDIR__ = os.path.abspath(os.path.join(_SCRIPT_DIR, '..'))

logger = logging.getLogger()

class spark_processor():

    def process_html_files(self, sparkContext, data_location):
        """
        Processes the clean HTML text files that were previously downloaded. Stores author, title, the article itself.
        :param sparkContext: the spark context object
        :param data_location: location of the input data files
        :return: None
        """
        sqlContext = SQLContext(sparkContext)
        first_pass = []

        for file in os.listdir(data_location):
            full_filepath = os.path.join(data_location, file)
            data = sparkContext.textFile(full_filepath)

            if data.isEmpty():
                logger.error("RDD IS EMPTY")

            # TODO: Put all site-specific items like search lines into a config, so we can search each config setting
            author = data.filter(lambda line: '<meta name="author"' in line.lower()).first()
            if author is None:
                author = data.filter(lambda line: '<meta property="article:author"' in line.lower()).first()

            title = data.filter(lambda line: '<title>' in line.lower()).take(1)[0]
            paragraphs = (data.filter(lambda line: '<p>' in line.lower()))

            # Clean Data, which needs to be plugged into the dataframe
            paragraphs = paragraphs.map(lambda item: self._clean_paragraph(item))
            paragraphs = paragraphs.reduce(lambda x, y: x + " " + y)
            title_clean, location = self._clean_title(title)
            author_clean = self._clean_author(author)


            first_pass.append((author_clean, title_clean, location, paragraphs))

        final_tuple = sparkContext.parallelize(first_pass)
        df = final_tuple.toDF(["Author", "Title", "Location", "Content"])
        # df = sqlContext.createDataFrame(clean_data_list, )
        df.collect()
        print(df.head())

        #This might not work locally, see: https://stackoverflow.com/questions/51603404/saving-dataframe-to-local-file-system-results-in-empty-results/51603898#51603898
        #df.write.csv(os.path.join(__WORKDIR__, "output",'out.csv'))

        df.toPandas().to_csv(os.path.join(__WORKDIR__, "output",'out.csv'))


    def _clean_paragraph(self, line):
        return soup(line, 'html.parser').text.strip(string.punctuation)


    def _clean_author(self, line):
        author = soup(line, 'html.parser').find("meta", {"name": "author"})["content"]
        return author


    def _clean_title(self, line):
        """
        Cleans the title string, currently only works for The Guardian
        :param line: the title string
        :return:
        """
        line_s = line.split(' | ')
        title_cleaned = soup(line_s[0], 'html.parser').text
        location = line_s[1].strip()
        return title_cleaned, location


    def __init__(self, spark_context_name, input_data_directory):

        conf = SparkConf().setAppName(spark_context_name)
        sc = SparkContext(conf=conf).getOrCreate()

        # TODO: Switch to config
        self.process_html_files(sc, input_data_directory)
