import argparse
from pyspark.sql.functions import udf
from pyspark.sql import functions as F
from pyspark import SparkContext, SparkConf
from pyspark.sql.context import SQLContext
# https://stackoverflow.com/questions/51226469/what-does-pyspark-need-psutil-for-faced-userwarning-please-install-psutil-to/51249740
import psutil
from bs4 import BeautifulSoup as soup
from pyspark.sql.types import *
from textblob import TextBlob
from operator import floordiv
# Is this the same as pyspark.sql.udf???

import os
import string
import logging
import pyspark.sql.functions
import pandas as pd

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


def _get_text_polarity(text):
    """
    Returns the polarity of the text ie. how positive, neutral, or negative the piece of text is.
    :param text: the text to analyze
    :return: the polarity of the text.
    """
    polarity = TextBlob(text).polarity

    if polarity < -0.10:
        return "Negative"

    if polarity > 0.10:
        return "Positive"

    return "Neutral"


def _get_top_freq_words(text):
    """
    Gets the top five most common words used in the given article.
    :param text: the article text
    :return top_words: an array of the most frequent words
    """
    # top_words = text.\
    #     flatMap(lambda x: x.split(' ')).\
    #     filter(lambda x: len(x) > 2).\
    #     map(lambda x: (x, 1)).\
    #     reduceByKey(lambda x, y: x + y).\
    #     map(lambda x: (x[1], x[0])).\
    #     sortByKey(ascending=False).take(5)

    dict = {}

    for word in text:
        if len(word) > 2:
            if word in dict.keys():
                dict[word] += 1
            else:
                dict[word] = 1

    x = [k for k, v in sorted(dict.items(), key=lambda item: item[1], reverse=True)]

    return x[:5]


def _get_and_clean_content(text, punctuation):
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
    # An arbitrary check, but should work for most abstracts in the guardian
    if len(abstract.split(" ")) < 8:
        abstract = paragraphs[2].text.lower()

    story = ''

    for para in paragraphs:
        _text = para.text
        if past_headers:

            # Removing non-ascii character hack.
            # https://stackoverflow.com/questions/1342000/how-to-make-the-python-interpreter-correctly-handle-non-ascii-characters-in-stri?noredirect=1&lq=1

            # Ideally any corpus cleaning and stop-word filtering should happen here.
            # https://datascience.stackexchange.com/questions/11402/preprocessing-text-before-use-rnn

            # Also Read:
            # https://link.springer.com/article/10.1007/s00799-018-0261-y

            _text = ''.join(s.lower() for s in _text if (s not in punctuation and ord(s) < 128))

            story = f"{story} {_text}"
        elif 'Last modified' in _text or 'First published' in _text:
            past_headers = True

    return abstract, story


def _get_and_clean_author(text):
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


def _get_and_clean_title(text):
    """
    Gets the title of the article and the geographical location it was published in
    :param text: the RDD for the webpage
    :return title_cleaned: the title
    :return location: the publish location
    """
    title_cleaned, location = " ", " "

    title = text.filter(lambda line: '<title>' in line.lower()).take(1)[0]

    line_s = title.split(' | ')
    title_cleaned = soup(line_s[0], 'html.parser').text
    location = line_s[1].strip()

    return title_cleaned, location


def clean_data(sparkContext, input_location):
    """
    For each file in input_location, we:
    1. Extract the Author
    2. Extract the Title
    3. Extract the Content
    4. Add this to our dataset.

    :param sparkContext: the Spark Context
    :param input_location: location of the input files
    :return raw_df: a PySpark DataFrame that contains clean data ready for processing.
    """
    sqlContext = SQLContext(sparkContext)

    logger.info("Starting Data Cleaning")

    punctuation = string.punctuation.join([",'$:."])
    raw_list = []

    for file in os.listdir(input_location):

        full_filepath = os.path.join(input_location, file)
        text = sparkContext.textFile(full_filepath)

        author = _get_and_clean_author(text)
        title, location = _get_and_clean_title(text)
        abstract, story = _get_and_clean_content(text, punctuation)

        logger.debug(f"{author}:{title}\n{abstract}\n\n")

        raw_list.append([author, title, location, abstract, story])

    # Parallelize and convert to DataFrame
    raw_rdd = sc.parallelize(raw_list)
    raw_df = sqlContext.createDataFrame(raw_rdd, ["Author", "Title", "Location", "Abstract", "Story"])
    return raw_df


def generate_insights(sparkContext, df):
    """
    Generates insights using the cleaned dataset. The list of insights are as follows:
    - Article Sentiment
    - Article Stance
    - Article Statistics (Top five words, word count, etc.)
    :param sparkContext: the spark context
    :param df: the data frame with clean data
    :return:
    """

    # Get the top five words for each story.
    top_words_udf = udf(_get_top_freq_words, ArrayType(StringType()))
    df = df.withColumn('Top_Five', top_words_udf(F.split(F.col('Story'), ' ')))

    # Get the word count for each article.
    df = df.withColumn('Word_Count', F.size(F.split(F.col('Story'), ' ')))

    # Get the polarity for each article.
    polarity_udf = udf(_get_text_polarity, StringType())
    df = df.withColumn('Polarity', polarity_udf(df['Story'])).show()

    # Write data to parquet file.
    df.write.parquet(os.path.join(__WORKDIR__, "output", "processed.parquet"))
    df.write.csv(os.path.join(__WORKDIR__, "output", "processed.csv"))


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

    df = clean_data(sc, args.input_location)
    generate_insights(sc, df)

