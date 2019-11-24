from bs4 import BeautifulSoup as soup
import sys
import requests

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


def get_page_url(url):
    """
    Get data from a page URL.
    :param url: the page URL
    :return:
    """
    page = requests.get(url)
    file_path = "..\\data\\test.txt"
    with open(file_path, 'w') as f:
        f.write(page.text)
    return file_path


if __name__ == "__main__":
    page_data = get_page_url("https://www.theguardian.com/us-news/2019/nov/20/trump-impeachment-hearings-gordon-sondland-testimony")
    read_page(page_data)