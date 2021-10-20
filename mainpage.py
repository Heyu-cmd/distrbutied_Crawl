import requests
import re
from threading import Thread
from redis_manager import RedisManager
class EnQueueUrl:
    def __init__(self):
        # page_list should store url which programe can use to crawl detail url
        self.page_list = []
        self.redis_manager = RedisManager()
    def get_list_page(self):
        """

        :return:url list
        """
        headers = {
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.71 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Language': 'zh-CN,zh;q=0.9',
        }
        response = requests.get('http://www.52xxit.com/forum.php', headers=headers, verify=False)
        html = response.content.decode("gbk")
        pattern = '<a href="(forum-.+?)">.+?</a>'
        kk = re.findall(pattern=pattern,string=html)

        for k in kk:
            self.page_list.append("http://www.52xxit.com/"+k)
    def get_html(self, url):

        headers = {
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.71 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Referer': 'http://www.52xxit.com/forum.php',
            'Accept-Language': 'zh-CN,zh;q=0.9',
        }

        response = requests.get(url, headers=headers, verify=False)
        return response.content.decode("gbk")

    def list2redis(self):
        """
        page is list url
        :return:
        """

        if self.page_list == []:
            return
        page = self.page_list.pop(0)
        html = self.get_html(page)
        pattern_url = '<a href="(thread-.+?-1-1.html)".+?>.+?</a>'
        pattern_page = '<a href="(forum-.+?html)">.+?</a>'
        domain_url = "http://www.52xxit.com/"
        url_list = re.findall(pattern=pattern_url, string=html)
        page_url = re.findall(pattern=pattern_page,string=html)
        for url in url_list:
            # write this url_ to redis
            url_ = domain_url + url
            print(url_)
            self.redis_manager.enqueueUrl(url_,"new")
        for page in page_url:
            # write this page_url to page_list
            page_ = domain_url + page
            if page_ not in self.page_list:
                self.page_list.append(page_)


if __name__ == '__main__':
    h1 = EnQueueUrl()
    h1.get_list_page()
    def three():
        global h1
        while True:
            h1.list2redis()
    t = Thread(target=three)
    t1 = Thread(target=three)
    t2 = Thread(target=three)
    t.start()
    t1.start()
    t2.start()