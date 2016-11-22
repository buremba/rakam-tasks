import json
import random
import boto3
import requests
import time

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('simple-key-value')


def clean_event(event):
    if event.get('CreatedDate') is None:
        print "no time, corrupted", event
        return

    if event.get('ContentType') == "Column":
        paths = event.get('Path').strip("/").split('/')
        event['_user'] = paths[1] if len(paths) > 1 else None
        event['category'] = paths[0]
        del event['Path']
    else:
        event['Path'] = event.get('Path')[1:-1].split("/")
    event['Title'] = event.get('Title').split(' ') if 'Title' in event else None
    event['_time'] = event.get('CreatedDate')
    event['Files'] = [f.get('FileUrl') for f in event.get('Files')] if 'Files' in event else None
    event['Description'] = event.get('Description').split(' ') if 'Description' in event else None
    del event['CreatedDate']
    if event.get('ContentType') is not None:
        del event['ContentType']
    if "ModifiedDate" in event:
        del event['ModifiedDate']
    return event


def fill_events(keys, events, collection, endpoint, select, try_count=4):
    cursor = table.get_item(
        Key={
            'project': 'hurriyet-api',
            'id': endpoint
        },
        AttributesToGet=['value']
    )
    if "Item" in cursor:
        cursor = cursor.get('Item').get('value') + 1
    else:
        cursor = 0

    new_cursor = cursor + 9500
    str_length = len(str(cursor))
    if str_length != len(str(new_cursor)):
        new_cursor = (10 ** str_length) - 1

    response = requests.get("https://api.hurriyet.com.tr/v1/" + endpoint, headers={'apikey': random.choice(keys)},
                            params={"$filter":
                                        "Id ge '{}' and Id le '{}'".format(str(int(cursor)).zfill(str_length),
                                                                           str(int(new_cursor)).zfill(
                                                                               str_length)) if cursor is not None else None,
                                    "$select": None, "$top": 50})

    if response.status_code == 439:
        time.sleep(60000)
    if response.status_code != 200:
        if try_count == 0:
            print '{} returned invalid response {}: {}'.format(endpoint, response.status_code, response.text)
            if response.status_code == 500:
                return new_cursor
        else:
            return fill_events(keys, events, collection, endpoint, select, try_count - 1)
    else:
        items = response.json()
        cur = None
        for event in items:
            properties = clean_event(event)
            if properties is None:
                continue

            events.append({'collection': collection, 'properties': properties})
            cur = max(int(properties.get('Id')), cur) if cur is not None else int(properties.get('Id'))
        if len(items) == 0:
            return new_cursor
            check_exists = requests.get("https://api.hurriyet.com.tr/v1/" + endpoint,
                                        headers={'apikey': random.choice(keys)},
                                        params={"$filter":
                                                    "Id ge '{}'".format(str(int(cursor)).zfill(
                                                        str_length)) if cursor is not None else None,
                                                "$select": None, "$top": 1})
            if check_exists.status_code != 200:
                print check_exists.text
                return

            if len(check_exists.json()) == 0:
                return new_cursor
            else:
                return cursor
        else:
            return cur


def collect_events(context, events, new_cursor, api_type=None):
    if len(events) == 0:
        return

    response = requests.post(context.get('rakam_api_url') + "/event/batch",
                             data=json.dumps(
                                 {'events': events, 'api': {'api_key': context.get('rakam_write_key')}}),
                             headers={'Content-type': 'application/json'})

    if response.status_code != 200:
        print('[{}] Invalid status code from Rakam {} with response {}'
              .format(api_type, response.status_code, response.text))
    else:
        print("[{}] collected {} events between {} and {}, new cursor is {}"
              .format(api_type, len(events), events[0].get('properties').get('_time'),
                      events[len(events) - 1].get('properties').get('_time'), new_cursor))


def fetch_events(context, collection, endpoint, select, try_count=4):
    for i in range(0, 4):
        events = []
        cursor = fill_events(context.get('hurriyet_api_keys'), events, collection, endpoint, select, try_count)

        if cursor is not None:
            collect_events(context, events, cursor, endpoint)

            table.update_item(
                Key={
                    'project': 'hurriyet-api',
                    'id': endpoint
                },
                UpdateExpression='SET #keyval = :val1',
                ExpressionAttributeNames={
                    '#keyval': 'value'
                },
                ExpressionAttributeValues={
                    ':val1': cursor
                }
            )


def fetch(event, function_context):
    fetch_events(event, 'article', 'articles', "Id,Title,CreatedDate,Path,Files,Description,Url")
    fetch_events(event, 'column', 'columns', "Id,CreatedDate,Path,Title,Description,Url")
    fetch_events(event, 'newsphotogallery', 'newsphotogalleries', "Id,CreatedDate,Description,Path,Files,Title,Url")


if __name__ == "__main__":
    class Context:
        def get_remaining_time_in_millis(self):
            return 100000


    with open('./event.json') as f:
        content = f.read()
        fetch(json.loads(content), Context())
