import argparse
import json
import os
import sys
import time
import urllib
from difflib import SequenceMatcher
from threading import Thread

import requests
from requests.exceptions import ConnectionError

parser = argparse.ArgumentParser(description='Sends your past Google search events to Rakam API.')
parser.add_argument('--rakam-collection', default="google_search",
                    help='Rakam write key')
parser.add_argument('--rakam-write-key',
                    help='Rakam write key')
parser.add_argument('--rakam-api-url', default="https://app.rakam.io",
                    help='Rakam API address')
parser.add_argument('--search-dir',
                    help='Your Google Takeout search history directory')
args = parser.parse_args()


def collect_event(context, events):
    response = requests.post(context.get('rakam_api_url') + "/event/batch",
                             data=json.dumps(
                                 {'events': events, 'api': {'api_key': context.get('rakam_write_key')}}),
                             headers={'Content-type': 'application/json'})

    if response.status_code != 200:
        print('[{}] Invalid status code from Rakam {} with response {}'
              .format('google search', response.status_code, response.text))
    else:
        print("[{}] collected {} events between {} and {}."
              .format('google search', len(events), events[0].get('properties').get('_time'),
                      events[len(events) - 1].get('properties').get('_time')))


term_category_cache = {}


def lookup_category(search_term, try_count=5):
    if search_term not in term_category_cache:
        response = None
        try:
            response = requests.get(
                "https://www.google.com/trends/api/autocomplete/" + urllib.quote_plus(
                    search_term.encode('utf-8')) + "?hl=en-EN&tz=-180")
        except ConnectionError as e:
            # Google Firewall doesn't like us
            pass

        if response is None or response.status_code != 200:
            if try_count > 0:
                if response is None or response.status_code == 413:
                    time.sleep((6 - try_count) * 3000)
                lookup_category(search_term, try_count - 1)
            else:
                print(search_term + " > category search response: " + str(
                    response.status_code if response is not None else 0))
                return None
        else:
            category_value = json.loads(response.text[6:]).get('default').get('topics')

            if len(category_value) > 0:
                similarity = [SequenceMatcher(None, topic.get('title'), search_term).ratio() for topic in
                              category_value]
                most_similar_item_similarity = max(similarity)
                if most_similar_item_similarity > 0.8:
                    category = category_value[similarity.index(most_similar_item_similarity)].get('type')
                    term_category_cache[search_term] = category
                else:
                    term_category_cache[search_term] = None
            else:
                term_category_cache[search_term] = None
    else:
        return term_category_cache.get(search_term)


def convert_google(context, search_file):
    events = []

    with open(search_file, 'r') as handler:
        raw_events = json.loads(handler.read()).get('event')
        for event in raw_events:
            query = event.get('query')
            search_term = query.get('query_text')

            category = lookup_category(search_term)

            for timestamps in query.get('id'):
                events.append({"collection": context.get('rakam_collection'), "properties":
                    {"search_term": search_term,
                     "_time": int(timestamps.get('timestamp_usec')) / 1000,
                     'category': category,
                     "_user": "1"}})
        collect_event(context, events)


if __name__ == "__main__":
    running_threads = []
    for f in os.listdir(args.search_dir):
        t = Thread(target=convert_google, args=({'rakam_api_url': args.rakam_api_url,
                                                 'rakam_collection': args.rakam_collection,
                                                 'rakam_write_key': args.rakam_write_key},
                                                os.path.join(args.search_dir, f),))
        t.daemon = True
        t.start()
        running_threads.append(t)

    for t in running_threads:
        try:
            t.join()
        except (KeyboardInterrupt, SystemExit):
            sys.exit()
