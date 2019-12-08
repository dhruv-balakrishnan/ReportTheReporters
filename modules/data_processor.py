from pyspark import SparkContext, SparkConf
from pyspark.sql.context import SQLContext
# https://stackoverflow.com/questions/51226469/what-does-pyspark-need-psutil-for-faced-userwarning-please-install-psutil-to/51249740
import psutil
from bs4 import BeautifulSoup as soup
from pyspark.sql.functions import udf
# Is this the same as pyspark.sql.udf???

import os
import string
import logging
import pandas

_SCRIPT_DIR = os.path.realpath(os.path.dirname(__file__))
__WORKDIR__ = os.path.abspath(os.path.join(_SCRIPT_DIR, '..'))

logger = logging.getLogger()

class spark_processor():

    def clean_html_files(self, sparkContext, data_location):
        """
        Cleans the text-only HTML files that were previously downloaded.
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
            # title = data.filter(lambda line: '<title>' in line.lower()).take(1)[0]
            # paragraphs = paragraphs.map(lambda item: self._clean_paragraph(item))
            # paragraphs = paragraphs.reduce(lambda x, y: x + " " + y)

            # Get Author
            author = soupified.find("meta", {"name": "author"})["content"]

            # Get title, and news region
            title = soupified.title.text
            title_clean, news_region = self._clean_title(title)

            # Get the abstract and the entire story
            paragraphs = soupified.find_all('p')
            abstract, story = self._clean_paragraph(paragraphs)
            # If we parallelize without splitting, spark will auto split by character, which is not what we want.
            story_rdd = self.sc.parallelize(story.split(' '))
            process_html_page(story_rdd)

            first_pass.append((author, title_clean, news_region, story))

        # Convert into Spark DataFrame for further processing.
        final_tuple = sparkContext.parallelize(first_pass)
        # A SQLContext or SparkSession is required for an RDD to have the toDF attribute
        self.df = final_tuple.toDF(["Author", "Title", "Location", "Content"])
        # df.collect()
        # print(df.head())

        #This might not work locally, see: https://stackoverflow.com/questions/51603404/saving-dataframe-to-local-file-system-results-in-empty-results/51603898#51603898
        #df.write.csv(os.path.join(__WORKDIR__, "output",'out.csv'))

        #df.toPandas().to_csv(os.path.join(__WORKDIR__, "output",'out.csv'))


    def _clean_paragraph(self, paragraphs):
        """
        Takes in an iterator containing all the paragraphs in a HTML page, and extracts and cleans the data
        :param paragraphs: the iterator for each paragraph section in a HTML page
        :return: abstract the abstract/subtitle for the page
        :return: story the full story, cleaned and ready for processing.
        """
        abstract = paragraphs[0].text
        story = ''

        past_headers = False

        for para in paragraphs:
            _text = para.text
            if past_headers:
                # Removing non-ascii character hack. https://stackoverflow.com/questions/1342000/how-to-make-the-python-interpreter-correctly-handle-non-ascii-characters-in-stri?noredirect=1&lq=1
                _text = ''.join(s for s in _text if ord(s) < 128 and s not in string.punctuation)
                story = f"{story} {_text}"
            elif 'Last modified' in _text or 'First published' in _text:
                past_headers = True
        return abstract, story


    def _clean_title(self, line):
        """
        Cleans the title string, currently only works for The Guardian
        :param line: the title string
        :return: title_cleaned the title of the page
        :return: location the region category where the story is published.
        """
        line_s = line.split(' | ')
        title_cleaned = soup(line_s[0], 'html.parser').text
        location = line_s[1].strip()
        return title_cleaned, location


    def __init__(self, spark_context_name, input_data_directory):
        self.df = None
        self.conf = SparkConf().setAppName(spark_context_name)
        self.sc = SparkContext(conf=self.conf).getOrCreate()
        self.main(input_data_directory)


    def main(self, input_data_directory):
        """
        Runs the processing workload
        :param input_data_directory: input folder for the clean files
        :return: None
        """
        # TODO: Switch to config someday
        self.clean_html_files(self.sc, input_data_directory)

        # The following lines wont work, as the SparkContext only works in the driver, and cannot be sent to a worker.
        #   sc = self.sc
        #   udf_process_data = udf(lambda x: process_html_page(x, sc), StringType())
        #   self.df.select('Author', udf_process_data('Content')).show(truncate=False)
        # This error will manifest:
        #   _pickle.PicklingError: Could not serialize object: Exception: It appears that you are attempting to
        #   reference SparkContext from a broadcast variable, action, or transformation. SparkContext can only be used
        #   on the driver, not in code that it run on workers. For more information, see SPARK-5063.

        # The following wont work, as the UDF requires that we pass a PySpark DataFrame,
        # which cannot be pickled (serialized)
        #   udf_process_data = udf(lambda x: process_html_page(x), StringType())
        #   self.df.select('Author', udf_process_data('Content')).show(truncate=False)


def process_html_page(content):
    """
    Processes a single html 'page', which is actually an RDD of cleaned article content.
    :param: content, an RDD of strings, which we can perform operations on
    :return: None
    """
    words_rdd = content.flatMap(lambda x: x.split(' ')).\
        map(lambda x: (x, 1)).\
        reduceByKey(lambda x, y: x + y).\
        map(lambda x: (x[1], x[0])).filter(lambda x: len(x[1]) > 4)
    print(words_rdd.collect()[:5])
