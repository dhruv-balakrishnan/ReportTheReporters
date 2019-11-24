from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import re
from bs4 import BeautifulSoup as soup
import os
import string


def main(sparkContext, data_location):

    clean_data_list = []

    for file in os.listdir(data_location):

        data = sparkContext.textFile(os.path.join(data_location, file))

        author = data.filter(lambda line: '<meta name="author"' in line.lower()).first()
        title = data.filter(lambda line: '<title>' in line.lower()).take(1)[0]
        paragraphs = (data.filter(lambda line: '<p>' in line.lower()))

        # Clean Data, which needs to be plugged into the dataframe
        paragraphs = paragraphs.map(lambda item: clean_paragraph(item))
        paragraphs = paragraphs.reduce(lambda x, y: x + " " + y)
        title_clean, location = clean_title(title)
        author_clean = clean_author(author)

        clean_data_list.append((author_clean, title_clean, location, paragraphs))


    sqlContext = SQLContext(sparkContext)
    df = sqlContext.createDataFrame(clean_data_list, ["Author", "Title", "Location", "Article"])
    print(df.printSchema())


def clean_paragraph(line):
    return soup(line, 'html.parser').text.strip(string.punctuation)


def clean_author(line):
    author = soup(line, 'html.parser').find("meta", {"name": "author"})["content"]
    return author


def clean_title(line):
    line_s = line.split(' | ')
    title_cleaned = soup(line_s[0], 'html.parser').text
    location = line_s[1].strip()
    return title_cleaned, location


if __name__ == "__main__":

    conf = SparkConf().setAppName("Test")
    sc = SparkContext(conf=conf).getOrCreate()

    # TODO: Switch to config
    main(sc, "..\\data\\clean")
