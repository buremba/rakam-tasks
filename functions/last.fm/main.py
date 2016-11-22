from __future__ import print_function

import json
import logging

import boto3

import requests

log = logging.getLogger()
log.setLevel(logging.DEBUG)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('simple-key-value')

artists = {}


def checkpoint(context, events, new_max_id):
    response = requests.post(context.get('rakam_api_url') + "/event/batch", data=json.dumps(
        {"events": events, "api": {"api_key": context.get('rakam_write_key'),
                                   "library": {"name": "lambda", "version": "1"}}})).json()
    if response != 1:
        raise Exception(response)

    table.update_item(
        Key={
            'project': 'last.fm',
            'id': 'scrobble'
        },
        UpdateExpression='SET #keyval = :val1',
        ExpressionAttributeNames={
            '#keyval': 'value'
        },
        ExpressionAttributeValues={
            ':val1': int(new_max_id)
        }
    )
    print("collected {} scrobbles".format(len(events)))


def handler(context, api_context, max_id=None, page=None, events=[], new_max_id=None):
    if max_id is None:
        cursor = table.get_item(
            Key={
                'project': 'last.fm',
                'id': 'scrobble'
            },
            AttributesToGet=['value']
        )
        if "Item" in cursor:
            max_id = int(cursor.get('Item').get('value'))
        else:
            max_id = 0

    recenttracksurl = 'http://ws.audioscrobbler.com/2.0/?method=user.getrecenttracks' \
                      + '&user={}&api_key={}&format=json&limit=200' \
                          .format(context.get('lastfm_username'), context.get('lastfm_api_key'))

    if page is not None:
        recenttracksurl += "&page=" + str(page)

    response = requests.get(recenttracksurl + "&from=" + str(max_id))

    response.raise_for_status()
    recenttracks = response.json().get('recenttracks')
    tracks = recenttracks.get('track')
    attr = recenttracks.get('@attr')

    for track in tracks:
        if (track.get("@attr") is not None and track.get("@attr").get("nowplaying")) or int(
                track.get("date").get("uts")) < max_id:
            continue

        track_time = int(track.get("date").get("uts"))
        properties = {
            "track_name": track.get("name"),
            "track_url": track.get("url"),
            "artist_name": track.get("artist").get("#text"),
            "album_name": track.get("album").get("#text"),
            "album_mbid": track.get("album").get("mbid"),
            "artist_mbid": track.get("artist").get("mbid"),
            "image": track.get("image")[1].get("#text") if track.get("image") is not None and len(
                track.get("image")) > 1 else None,
            "mbid": track.get("mbid"),
            "id": track.get("id"),
            "_time": track_time * 1000
        }

        new_max_id = track_time if new_max_id is None else max(track_time, new_max_id)

        artist_name = track.get("artist").get("#text")
        if artist_name in artists:
            artist_info = artists[artist_name]
        else:
            rich_resp = requests.get(
                'http://ws.audioscrobbler.com/2.0/?method=artist.getInfo'
                '&api_key=2492cd052acd3d6675c3ae0ad3416b13&format=json&artist=' + artist_name)

            if rich_resp.status_code == 200:
                artist_info = rich_resp.json().get("artist")
                artists[artist_name] = artist_info

        if artist_info is not None:
            properties["artist_tags"] = [tag.get("name") for tag in artist_info.get("tags").get("tag")]
            properties["artist_listeners"] = int(artist_info.get("stats").get("listeners"))
            properties["artist_playcount"] = int(artist_info.get("stats").get("playcount"))

        events.append({
            "collection": "lastfm_scrobble",
            "properties": properties
        })

    current_page = int(attr.get('page'))
    if current_page < int(attr.get('totalPages')):
        if len(events) > 10000:
            checkpoint(context, events, new_max_id)
            events = []
            new_max_id = None
        return handler(event, context, max_id=max_id, page=current_page + 1, events=events, new_max_id=new_max_id)
    else:
        checkpoint(context, events, new_max_id)
        print("processed " + attr.get('totalPages') + " pages")


if __name__ == "__main__":
    class Context:
        def get_remaining_time_in_millis(self):
            return 100000


    with open('./event.json') as f:
        content = f.read()
        handler(json.loads(content), Context())
