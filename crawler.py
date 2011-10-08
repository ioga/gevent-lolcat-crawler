import os
import sys

from gevent import Greenlet, Timeout
import gevent.monkey
import gevent.pool
import gevent.queue
gevent.monkey.patch_socket()

from urllib2 import urlopen, URLError
from urlparse import urlsplit, urljoin
from HTMLParser import HTMLParser, HTMLParseError
import mimetypes
import argparse

import logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger()

TMPDIR = 'tmpdir'
JOBS = 5

class HTMLLinkExtractor(HTMLParser):
    def __init__(self, url, queue):
        HTMLParser.__init__(self)
        self.url = url
        self.queue = queue

    def handle_starttag(self, tag, attrs):
        try: self._add_link(dict(attrs)[{ 'a': 'href', 'img': 'src', }[tag]])
        except KeyError: pass

    def _add_link(self, ref_url):
        new_url = urljoin(self.url, ref_url)
        log.info('Adding to queue: {0}'.format(new_url))
        self.queue.put(new_url)

    def reset(self):
        HTMLParser.reset(self)

    def extract_links(self):
        self.reset()
        self.feed(urlopen(self.url).read())

def isimage(url):
    guess = mimetypes.guess_type(url)[0]
    return guess.startswith('image') if guess else False

def fetch_page(url, q):
    try:
        HTMLLinkExtractor(url, q).extract_links()
        log.info('Parsed page: {0}'.format(url))
    except (HTMLParseError, URLError), ex:
        log.exception(ex)

def fetch_image(url, tmpdir, bufsize=8192):
    filepath = os.path.join(tmpdir, os.path.basename(url))
    try:
        with open(filepath, 'wb') as f:
            for chunk in urlopen(url).read(bufsize):
                f.write(chunk)
        log.info('Fetched image: {0}'.format(filepath))
    except (URLError, IOError), ex:
        log.exception(ex)
        try: # Remove unfinished file
            os.unlink(filepath)
        except (IOError, OSError), ex:
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
                pool.spawn(fetch_page, url, q)
    pool.join()

class PageFetcher(Greenlet):
    def __init__(self, url, queue):
        Greenlet.__init__(self)
        self.url = url
        self.queue = queue

    def _run(self):
        with Timeout(20):
            try:
                HTMLLinkExtractor(self.url, self.queue).extract_links()
                log.info('Parsed page: {0}'.format(self.url))
            except (HTMLParseError, URLError), ex:
                log.error('Exception on url: {0}'.format(self.url))
                log.exception(ex)

class ImageFetcher(Greenlet):
    def __init__(self, url, dir, bufsize=8192):
        Greenlet.__init__(self)
        self.url = url
        self.dir = dir
        self.bufsize = bufsize

    def _run(self):
        url, dir, bs = self.url, self.dir, self.bufsize
        with Timeout(10):
            filepath = os.path.join(dir, os.path.basename(url))
            try:
                with open(filepath, 'wb') as f:
                    for chunk in urlopen(url).read(bs):
                        f.write(chunk)
                log.info('Fetched image: {0}'.format(filepath))
            except (URLError, IOError), ex:
                log.error('Exception on url: {0}'.format(self.url))
                log.exception(ex)
                try: # Remove unfinished file
                    os.unlink(filepath)
                except (IOError, OSError), ex:
                    log.exception(ex)

class Crawler(object):
    def __init__(self, start_url, concurrency, target_dir):
        self.hostname = urlsplit(start_url).hostname
        self.pool = gevent.pool.Pool(concurrency)
        self.queue = gevent.queue.Queue()
        self.visited = set()
        self.jobs = concurrency
        self.dir = target_dir

        self.queue.put(start_url)

    def run(self):
        while not self.queue.empty() or self.pool.free_count() != self.jobs:
            try: url = self.queue.get(timeout=0.5)
            except gevent.queue.Empty:
                log.info('Queue is empty')
                continue
            if url not in self.visited and self.hostname in url:
                self.visited.add(url)
                if self._is_image(url):
                    self.pool.start(ImageFetcher(url, self.dir))
                else:
                    self.pool.start(PageFetcher(url, self.queue))
        self.pool.join()

    @staticmethod
    def _is_image(url):
        guess = mimetypes.guess_type(url)[0]
        return guess.startswith('image') if guess else False

def test():
    # Mock urlopen
    def my_urlopen(url):
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

    # Clean all files from target directory
    for file in os.listdir(TMPDIR):
        os.unlink(os.path.join(TMPDIR, file))

    timeout = Timeout(15)
    try:
        timeout.start()
        Crawler('http://test_site', JOBS, TMPDIR).run()
    except Timeout:
        sys.exit('Test timeout')
    finally:
        timeout.cancel()

    import filecmp
    dcmp = filecmp.dircmp('test_site/cats', TMPDIR)
    same = set(dcmp.same_files)
    for file in dcmp.left_list:
        if file not in same:
            sys.exit('Test failed. Cat file {0} is absent or differs in target directory'.format(file))

    print 'Test OK.'

def main():
    #test()
    #return
    #crawler('http://halibut.su', 5, '')
    parser = argparse.ArgumentParser(description='Recursively crawls target site and fetches all images into target directory')
    parser.add_argument('url', metavar='URL', nargs='?')
    parser.add_argument('-d', metavar='DIR', help='Target directory, defaults to {0}'.format(TMPDIR), default=TMPDIR)
    parser.add_argument('-j', metavar='JOBS', help='Number of workers, defaults to {0}'.format(JOBS), default=JOBS)
    parser.add_argument('-t', action='store_true', help='Run test')
    args = parser.parse_args()

    if args.t:
        test()
        return

    if not os.path.isdir(args.d):
        sys.exit("Target directory doesn't exists")

    if not args.url:
        parser.print_help()
        sys.exit('Missing URL')

    Crawler(args.url, args.j, args.d).run()

if __name__ == '__main__':
    main()
