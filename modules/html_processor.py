import argparse

from pyspark import SparkContext, SparkConf
from pyspark.sql.context import SQLContext
# https://stackoverflow.com/questions/51226469/what-does-pyspark-need-psutil-for-faced-userwarning-please-install-psutil-to/51249740
import psutil
from bs4 import BeautifulSoup as soup
from textblob import TextBlob
from operator import floordiv
# Is this the same as pyspark.sql.udf???

import os
import string
import logging
import pyspark.sql.functions
import pandas

_SCRIPT_DIR = os.path.realpath(os.path.dirname(__file__))
__WORKDIR__ = os.path.abspath(os.path.join(_SCRIPT_DIR, '..'))

logger = logging.getLogger()

def clean_html_files(sparkContext, data_location):
    """
    Cleans the text-only HTML files that were previously downloaded.
    :param sparkContext: the spark context object
    :param data_location: location of the input data files
    :return: None
    """
    sqlContext = SQLContext(sparkContext)
    first_pass = []
    counter = 0

    for file in os.listdir(data_location):
        counter += 1
        print(counter)
        full_filepath = os.path.join(data_location, file)
        data = sparkContext.textFile(full_filepath)

        with open(full_filepath, 'r', encoding='utf-8') as f:
            soupified = soup(f.read(), 'html.parser')

        # For Sentiment Analysis
        abstract_punct, story_punct = _clean_paragraph(paragraphs, False)

        # blob = TextBlob(story)
        # polarity, sentiment = blob.sentiment
        # if polarity * 100 > 70:
        #     print("Positive")
        # elif 30 < polarity * 100 < 70:
        #     print("Neutral")
        # else:
        #     print("Negative")
        # print(f"Polarity: {polarity}, Sentiment: {sentiment}")
        # Requires training
        # print(f"Classification: {blob.classify()}")

        # If we parallelize without splitting, spark will auto split by character, which is not what we want.
        # story_rdd = sc.parallelize(story.split(' '))


        first_pass.append((author, title_clean, news_region, story))

    # Convert into Spark DataFrame for further processing.
    final_tuple = sparkContext.parallelize(first_pass)
    # A SQLContext or SparkSession is required for an RDD to have the toDF attribute
    df = final_tuple.toDF(["Author", "Title", "Location", "Content"])
    # df.collect()
    # print(df.head())

    #This might not work locally, see: https://stackoverflow.com/questions/51603404/saving-dataframe-to-local-file-system-results-in-empty-results/51603898#51603898
    #df.write.csv(os.path.join(__WORKDIR__, "output",'out.csv'))

    #df.toPandas().to_csv(os.path.join(__WORKDIR__, "output",'out.csv'))



def main(input_data_directory):
    """
    Runs the processing workload
    :param input_data_directory: input folder for the clean files
    :return: None
    """
    # TODO: Switch to config someday
    clean_html_files(sc, input_data_directory)

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

    # Map Sentiments for the words. However, this isn't really helpful and doesn't say anything about the author. See
    # below for a better solution.
    # sentiment_rdd = content.\
    #     flatMap(lambda x: x.split(' ')).\
    #     map(lambda x: (x, TextBlob(x).polarity * 100)).\
    #     reduceByKey(lambda x, y: floordiv((x+y), 2) if x + y > 0 else (x+y)).\
    #     map(lambda x: (x[1], x[0])).\
    #     filter(lambda x: len(x[1]) > 2).\
    #     sortByKey(ascending=False)

    words_rdd = content.\
        flatMap(lambda x: x.split(' ')).\
        map(lambda x: (x, 1)).\
        reduceByKey(lambda x, y: x+y).\
        map(lambda x: (x[1], x[0])).\
        filter(lambda x: len(x[1]) > 2).\
        sortByKey(ascending=False).\
        limit(10).\
        map(lambda x, y: (y, TextBlob(y).polarity))


    print(f"\n{words_rdd.take(10)}")


def get_and_clean_content(text):
    """
    Gets the content (paragraphs) of the article, and returns a cleaned version.
    :param text: the RDD of the article
    :return abstract: The abstract of the story
    :return story: The full, cleaned, story.
    """
    whole_text = text.flatMap(lambda x: x.split(" ")).reduce(lambda x, y: x + " " + y)
    paragraphs = soup(whole_text, 'html.parser').find_all('p')

    past_headers = False
    abstract = paragraphs[0].text
    story = ''
    for para in paragraphs:
        _text = para.text
        if past_headers:

            # Removing non-ascii character hack.
            # https://stackoverflow.com/questions/1342000/how-to-make-the-python-interpreter-correctly-handle-non-ascii-characters-in-stri?noredirect=1&lq=1

            # TODO: Figure out why punctuation is still showing up. Example:
            #  Innocent teen George Stinney was tried, convicted and executed in 83 days in Jim Crow south of 1944:
            #  ‘a truly unfortunate episode in our history’
            _text = ''.join(s for s in _text if s not in string.punctuation and ord(s) < 128)

            # Ideally any corpus cleaning and stop-word filtering should happen here.
            # https://datascience.stackexchange.com/questions/11402/preprocessing-text-before-use-rnn

            story = f"{story} {_text}"
        elif 'Last modified' in _text or 'First published' in _text:
            past_headers = True

    return abstract, story


def get_and_clean_author(text):
    """
    Gets the author of the article.
    :param text: the RDD for the webpage
    :return author_clean: the name of the author
    """

    author_clean = None
    try:
        try:
            author = text.filter(lambda line: '<meta name="author"' in line.lower()).first()
            author_clean = soup(author, 'html.parser').find("meta", {"name": "author"})["content"]
        except ValueError:
            author = text.filter(lambda line: '<meta property="article:author"' in line.lower()).first()
            author_clean = soup(author, 'html.parser').find("meta", {"property": "article:author"})["content"]
    except Exception as E:
        logger.error(f"Reached unexpected exception: {E}")


    return author_clean


def get_and_clean_title(text):
    """
    Gets the title of the article and the geographical location it was published in
    :param text: the RDD for the webpage
    :return title_cleaned: the title
    :return location: the publish location
    """

    title = text.filter(lambda line: '<title>' in line.lower()).take(1)[0]

    line_s = title.split(' | ')
    title_cleaned = soup(line_s[0], 'html.parser').text
    location = line_s[1].strip()

    return title_cleaned, location


def start_processing(sparkContext, input_location):
    """
    For each file in input_location, we need to first:
    1. Extract the Author
    2. Extract the Title
    3. Extract the Content
    4. Store these in an RDD.

    Then, ???
    :param sparkContext: the Spark Context
    :param input_location: location of the input files
    :return: None
    """
    sqlContext = SQLContext(sparkContext)

    logger.info("Starting Data Cleaning")

    for file in os.listdir(input_location):

        full_filepath = os.path.join(input_location, file)
        text = sparkContext.textFile(full_filepath)

        author = get_and_clean_author(text)
        title, location = get_and_clean_title(text)
        abstract, story = get_and_clean_content(text)

        logger.debug(f"{author}:{title}\n{abstract}")



if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Process html files')

    parser.add_argument('--input_location',
                        dest='input_location',
                        type=str,
                        default=os.path.join(__WORKDIR__, "data", "clean"),
                        help='Input location for the html files')
    parser.add_argument('--spark_context_name',
                        dest='spark_context_name',
                        type=str,
                        default="dudewhat",
                        help='Name of the Spark context')



    args = parser.parse_args()

    conf = SparkConf().setAppName(args.spark_context_name)
    sc = SparkContext(conf=conf).getOrCreate()

    start_processing(sc, args.input_location)

