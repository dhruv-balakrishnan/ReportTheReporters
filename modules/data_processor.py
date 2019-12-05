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
            # data = sparkContext.textFile(full_filepath)

            with open(full_filepath, 'r', encoding='utf-8') as f:
                soupified = soup(f.read(), 'html.parser')

            # TODO: Put all site-specific items like search lines into a config, so we can search each config setting

            # author = data.filter(lambda line: '<meta name="author"' in line.lower()).first()
            # if author is None:
            #     author = data.filter(lambda line: '<meta property="article:author"' in line.lower()).first()
            #title = data.filter(lambda line: '<title>' in line.lower()).take(1)[0]
            # paragraphs = paragraphs.map(lambda item: self._clean_paragraph(item))
            # paragraphs = paragraphs.reduce(lambda x, y: x + " " + y)

            author = soupified.find("meta", {"name": "author"})["content"]

            title = soupified.title.text
            title_clean, news_region = self._clean_title(title)

            paragraphs = soupified.find_all('p')
            abstract, story = self._clean_paragraph(paragraphs)
            first_pass.append((author, title_clean, news_region, story))

        # Convert into Spark DataFrame for further processing.
        final_tuple = sparkContext.parallelize(first_pass)
        # An SQLContext or SparkSession is required for an RDD to have the toDF attribute
        df = final_tuple.toDF(["Author", "Title", "Location", "Content"])
        # df.collect()
        # print(df.head())

        #This might not work locally, see: https://stackoverflow.com/questions/51603404/saving-dataframe-to-local-file-system-results-in-empty-results/51603898#51603898
        #df.write.csv(os.path.join(__WORKDIR__, "output",'out.csv'))

        #df.toPandas().to_csv(os.path.join(__WORKDIR__, "output",'out.csv'))


    def _clean_paragraph(self, paragraphs):
        abstract = paragraphs[0].text
        story = ''

        past_headers = False

        for para in paragraphs:
            _text = para.text
            if past_headers:
                _text = _text.strip(string.punctuation)
                story = f"{story}|{_text}"
            elif 'Last modified' in _text or 'First published' in _text:
                past_headers = True
        return abstract, story


    def _clean_title(self, line):
        """
        Cleans the title string, currently only works for The Guardian
        :param line: the title string
        :return: a clean title and a news location
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
