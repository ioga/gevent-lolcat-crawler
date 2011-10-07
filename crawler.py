import os
import gevent
import gevent.monkey
import gevent.pool
import gevent.queue
gevent.monkey.patch_socket()
from urllib2 import urlopen
from urlparse import urlsplit, urljoin
import logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger()
from HTMLParser import HTMLParser
import mimetypes

class HTMLLinkExtractor(HTMLParser):
    def __init__(self):
        HTMLParser.__init__(self)
        self.links = []
    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            self.links.append(dict(attrs).get('href'))
        if tag == 'img':
            self.links.append(dict(attrs).get('src'))

    def reset(self):
        HTMLParser.reset(self)
        self.links = []

    def extract_links(self, body, url):
        self.reset()
        self.feed(body)
        for link in self.links:
            yield urljoin(url, link)

def isimage(url):
    guess = mimetypes.guess_type(url)[0]
    if guess:
        return guess.startswith('image')
    else:
        return False


def fetch(url, q):
    extractor = HTMLLinkExtractor()
    try:
        for new_url in extractor.extract_links(urlopen(url).read(), url):
            log.warning('Adding url: {0}'.format(new_url))
            q.put(new_url)
    except Exception, ex:
        log.exception(ex)

def fetch_image(url, tmpdir, bufsize=8192):
    filepath = os.path.join(tmpdir, os.path.basename(url))
    try:
        with open(filepath, 'wb') as f:
            for chunk in urlopen(url).read(bufsize):
                f.write(chunk)
    except Exception, ex:
        os.unlink(filepath)
        log.exception(ex)


def crawler(start_url, concurrency, tmpdir):
    hostname = urlsplit(start_url).hostname
    pool = gevent.pool.Pool(concurrency)
    q = gevent.queue.Queue()
    visited = set()
    q.put(start_url)
    while not q.empty() or pool.free_count() != concurrency:
        try: url = q.get(timeout=0.5)
        except gevent.queue.Empty:
            log.info('Queue is empty')
            continue
        if url not in visited and hostname in url:
            visited.add(url)
            if isimage(url):
                pool.spawn(fetch_image, url, tmpdir)
            else:
                pool.spawn(fetch, url, q)
    pool.join()

def test():
    def my_urlopen(url):
        import os
        url_path = urlsplit(url).path.lstrip('/')
        real_path = os.path.join('test_site', url_path)
        if os.path.isdir(real_path):
            real_path = os.path.join(real_path, 'index.html')
        if os.path.isfile(real_path):
            return open(real_path, 'rb')
        else:
            raise IOError('Error 404: Not Found')
    global urlopen
    urlopen = my_urlopen
    crawler('http://test_site', 2, 'tmpdir')

def main():
    test()
    #crawler('http://halibut.su', 5, '')

if __name__ == '__main__':
    main()
