import threading
import functools
import string
import queue
import time
import json

import requests
import bs4


def threadify(func):
    "function runs in a new thread."

    @functools.wraps(func)
    def run(*args, **kwds):
        new_thread = threading.Thread(
            target = func,
            args   = args,
            kwargs = kwds)
        new_thread.start()
        return new_thread

    return run

def load_names():
    
    def get_names(url):
        resp = requests.get(url)
        if resp.status_code != 200:
            print("Error:", resp.text)
            return [], url # try again!
        soup = bs4.BeautifulSoup(resp.text, 'html.parser')
        page = soup.find_all(class_="mw-category-group")
        data = [(i.text, i["href"]) for e in page for i in e.find_all("a")]
        next_url = soup.find("a", text="next page")
        return data, next_url['href'] if next_url else None

    @threadify
    def collect(url, collector):
        out = []
        try:
            while url:
                out, url = get_names('https://en.wikipedia.org' + url)
                if out[0] in collector:
                    break
                collector.update(out)
            print("breakpoint reached!")
        except:
            # if there's an error just collect(url, collector)
            # again once there's space for another thread
            print("error when collecting:", url)

    def main():
        LIVING_PEOPLE_MAIN_PAGE = "/wiki/Category:Living_people"
        pages = [LIVING_PEOPLE_MAIN_PAGE]
        for c in string.ascii_uppercase:
            pages.append(f"/wiki/Category:Living_people?from={c}")
        results = set()
        threads = []
        for p in pages:    
            threads.append(collect(p, results))

        return results, threads
    
    return main()


def get_visit_count(links, results=None):

    def get_view_counts(link):
        name = link.replace("/wiki/", "")
        end  = "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/"
        url  = f"en.wikipedia.org/all-access/all-agents/{name}/monthly/20180101/20181231"
        view = json.loads(requests.get(end+url).text)
        if "items" in view:
            return sum(i["views"] for i in view["items"])
        else:
            return None
        
    @threadify
    def loader(q, results):
        link = q.get()
        error_count = 0
        while q:
            if link in results:
                link = q.get()
                error_count = 0
                continue
            try:
                results[link] = get_view_counts(link)
            except BaseException as e:
                print("Error: retrying", link, e)
                time.sleep(2)
                error_count += 1
            else:
                link = q.get()
                error_count = 0
            if error_count > 30:
                link = q.get()
                error_count = 0


    def main(links, results=None):
        if results is None:
            results = {}
        q = queue.Queue()
        for i in sorted(links):
        	q.put(i)
        threads = []
        for _ in range(10):
            t = loader(q, results)
            threads.append(t)
        return results, threads
            
    return main(links, results)
