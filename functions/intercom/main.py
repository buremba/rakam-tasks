import json

import boto3

import requests
from intercom import User, Intercom, Conversation
from requests.auth import HTTPBasicAuth

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('simple-key-value')


def fetch_new_conversation(context):
    cursor = table.get_item(
        Key={
            'project': 'intercom',
            'id': 'conversation'
        },
        AttributesToGet=['value']
    )
    if "Item" in cursor:
        max_id = cursor.get('Item').get('value')
    else:
        max_id = 0

    events = []
    for convo in Conversation.find_all(after=max_id):
        event = {
            'assignee_type': None,
            'assignee_id': convo.assignee.id,
            'subject': convo.conversation_message.subject,
            'author_id': convo.conversation_message.author.id,
            'author_type': None,
            '_time': convo.created_at.isoformat("T"),
            '_user': convo.user.user_id or convo.user.email or convo.user.id
        }
        max_id = max(int(convo.id), max_id)
        events.append({'collection': context.get('collection_prefix') + '_conversation', 'properties': event})

    response = requests.post(context.get('rakam_api_url') + "/event/batch",
                             data=json.dumps(
                                 {'events': events, 'api': {'api_key': context.get('rakam_write_key')}}),
                             headers={'Content-type': 'application/json'})

    if len(events) == 0:
        return

    if response.status_code != 200:
        print('[event] Invalid status code from Rakam {} with response {}'
              .format(response.status_code, response.text))
    else:
        print("[event] collected {} events between {} and {}, new cursor is {}"
              .format(len(events), events[0].get('properties').get('_time'),
                      events[len(events) - 1].get('properties').get('_time'), max_id))

    table.update_item(
        Key={
            'project': 'intercom',
            'id': 'conversation'
        },
        UpdateExpression='SET #keyval = :val1',
        ExpressionAttributeNames={
            '#keyval': 'value'
        },
        ExpressionAttributeValues={
            ':val1': max_id
        }
    )


def fetch_all_users(context):
    data = []
    for user in User.all():
        event = {
            'picture_url': user.avatar.image_url,
            'companies': user.companies,
            'created_at': user.signed_up_at.isoformat("T"),
            'intercom_id': user.id,
            'session_count': user.session_count,
            'tags': map(lambda tag: tag.id, user.tags),
            'segments': map(lambda segment: segment.id, user.segments),
            'unsubscribed_from_emails': user.unsubscribed_from_emails,
            'updated_at': user.updated_at.isoformat("T"),
            '_user_agent': user.user_agent_data
        }

        event.update(user.location_data.attributes)

        data.append({'id': user.user_id or user.email or user.id, 'set_properties': event})

        if False:
            response = requests.get("https://api.intercom.io/events?type=user&intercom_user_id=" + user.id,
                                    auth=HTTPBasicAuth(context.get('intercom_app_id'), context.get('intercom_api_key')),
                                    headers={'Accept': 'application/json'})

            if response.status_code == 200:
                event_data = response.json()
                for event in event_data.get('events'):
                    pass

    response = requests.post(context.get('rakam_api_url') + "/user/batch_operations",
                             json.dumps({'api': {'api_key': context.get('rakam_write_key'),
                                                 'library': {'name': 'rakam-task-intercom', 'version': '0.1'}},
                                         'data': data}))

    if response.status_code != 200:
        print('[{}] Invalid status code from Rakam {} with response {}'
              .format('user', response.status_code, response.text))
    else:
        print("{} users are updated".format(len(data)))


def fetch(event, function_context):
    Intercom.app_id = event.get('intercom_app_id')
    Intercom.app_api_key = event.get('intercom_api_key')

    fetch_all_users(event)
    fetch_new_conversation(event)


if __name__ == "__main__":
    class Context:
        def get_remaining_time_in_millis(self):
            return 100000


    with open('./event.json') as f:
        content = f.read()
        fetch(json.loads(content), Context())
