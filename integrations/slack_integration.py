import os
from slack_sdk import WebClient
from ratelimit import limits, sleep_and_retry
from slack_sdk.errors import SlackApiError
from typing import Any, Dict, List, Optional
from datetime import datetime
import pandas as pd
from uuid import uuid4
import json


# TODO : while getting the messages now we are getting just the parent messages , using conversations.replies api call
# TODO: refactoring
# TODO: number of replies
# FIXME : PROBLEM with threads and user score , many times we do not use thread and comment directly below the post to reply and initialte conversation , now
# my problem with that is how to diffrentiate b/w post and comments/replies , because if we take threaded message as post there is probablity we mis the above case
# TODO : apply `ratelimit` wherever required
def _get_human_datetime(ts):
    return datetime.fromtimestamp(int(float(ts))).strftime("%d/%m/%Y, %H:%M:%S")


def build_channels_payload(channels):
    return {
        channel["id"]: {
            "id": channel["id"],
            "name": channel["name"],
            "created_by": channel["creator"],
            "created_on": datetime.fromtimestamp(channel["created"]).strftime(
                "%d/%m/%Y, %H:%M:%S"
            ),
            "is_archived": channel["is_archived"],
            "topic": channel["topic"],
            "is_org_shared": channel["is_org_shared"],
        }
        for channel in channels
    }


def build_slack_payload(message, channel_id, is_post):
    conversation = {}
    conversation["id"] = str(uuid4())
    conversation["client_msg_id"] = (
        message["client_msg_id"] if "client_msg_id" in message.keys() else ""
    )
    conversation["msg_type"] = (
        message["msg_type"] if "msg_type" in message.keys() else ""
    )
    conversation["text"] = message["text"]
    conversation["sent_by"] = message["user"]
    conversation["ts"] = message["ts"]
    conversation["channel"] = channel_id
    conversation["sent_on"] = _get_human_datetime(message["ts"])
    conversation["files"] = message["files"] if "files" in message.keys() else ""
    conversation["num_files"] = (
        len(message["files"]) if "files" in message.keys() else 0
    )
    conversation["reply_count"] = (
        message["reply_count"] if "reply_count" in message.keys() else 0
    )
    conversation["reactions"] = (
        len(message["reactions"]) if "reactions" in message.keys() else 0
    )
    conversation["latest_reply_ts"] = (
        message["latest_reply"] if "latest_reply" in message.keys() else ""
    )
    conversation["attachments"] = (
        message["attachments"] if "attachments" in message.keys() else ""
    )
    conversation["is_post"] = is_post
    conversation["is_engaging_post"] = False
    if "thread_ts" in message.keys() or "attachments" in message.keys():
        conversation["is_engaging_post"] = True
    conversation["team"] = message["team"] if "team" in message.keys() else ""
    conversation["blocks"] = message["blocks"] if "blocks" in message.keys() else ""
    conversation["thread_ts"] = (
        message["thread_ts"] if "thread_ts" in message.keys() else ""
    )
    conversation["parent_user_id"] = (
        message["parent_user_id"] if "parent_user_id" in message.keys() else ""
    )
    conversation["subscribed"] = (
        message["subscribed"] if "latest_reply" in message.keys() else ""
    )
    conversation["total_activity"] = (
        len(conversation["attachments"])
        + conversation["reply_count"]
        + conversation["reactions"]
    )

    return conversation


class SlackGetterConfig:
    def __init__(self, token_id, channels=None, lookup_time="now"):
        self.token_id = token_id
        self.lookup_duration = lookup_time
        self.channels = channels


API_TIME_LIMITER = 60


class SlackGetter:
    def __init__(self, config):
        self.client = WebClient(token=config.token_id)
        self.init_scraper()

    def init_scraper(self):
        self.USERS = self._get_users()
        self.CHANNELS = self._get_channels_()

    # TODO : add the checks of success and try except block
    def _add_app_to_channel(self, channel_id):
        self.client.conversations_join(channel=channel_id)

    def _get_users(self):
        users_output = self.client.users_list()
        if users_output["ok"] == False:
            print("ERROR")
            return
        users_list = users_output["members"]
        return {
            user["id"]: {
                "user_id": user["id"],
                "team_id": user["team_id"],
                "profile_name": user["name"],
                "full_name": user["profile"]["real_name"],
                "display_name": user["profile"]["display_name"],
                "user_title": user["profile"]["title"],
                "status_text": user["profile"]["status_text"],
                "status_emoji": user["profile"]["status_emoji"],
                "email": user["profile"]["email"]
                if "email" in user["profile"].keys()
                else "",
                "first_name": user["profile"]["first_name"],
                "last_name": user["profile"]["last_name"],
                "is_admin": user["is_admin"],
                "is_owner": user["is_owner"],
                "last_updated": _get_human_datetime(user["updated"]),
                "is_restricted": user["is_restricted"],
            }
            for user in users_list
            if user["is_bot"] == False
        }

    def _get_channels_(self):
        conversations_response = self.client.conversations_list()
        channels = conversations_response["channels"]
        return {
            channel["id"]: {
                "id": channel["id"],
                "name": channel["name"],
                "created_by": channel["creator"],
                "created_on": _get_human_datetime(channel["created"]),
                "is_archived": channel["is_archived"],
                "topic": channel["topic"],
                "is_org_shared": channel["is_org_shared"],
                "users": self._get_channel_members(channel["id"]),
            }
            for channel in channels
        }

    @sleep_and_retry
    @limits(calls=100, period=API_TIME_LIMITER)
    def _get_threaded_messages(self, channel_id, thread_ts):

        thread_respnse = self.client.conversations_replies(
            channel=channel_id, ts=thread_ts
        )
        if thread_respnse["ok"] == True:
            return thread_respnse["messages"]
        return []

    def _get_channel_members(self, channel_id):
        channel_members = self.client.conversations_members(channel=channel_id)
        user_ids = channel_members["members"] if channel_members["ok"] == True else []
        return user_ids

    def _get_channel_messages(self, channel_id, limit=1000):
        # self.CHANNELS["channel_id"] #TODO: if bot not in messages
        messages_result = self.client.conversations_history(
            channel=channel_id, limit=limit
        )
        conversation_history = (
            messages_result["messages"] if messages_result["ok"] == True else []
        )

        conversations = []
        for message in conversation_history:
            if "thread_ts" in message.keys():
                thread_response = self._get_threaded_messages(
                    channel_id, message["thread_ts"]
                )
                for response in thread_response:
                    conversations.append(
                        build_slack_payload(response, channel_id, is_post=False)
                    )
            else:
                conversations.append(
                    build_slack_payload(message, channel_id, is_post=True)
                )

        # FIXME : For Poc using pandas dataframe, move to a stable db/ config later
        df = pd.DataFrame(conversations)
        df["ts"] = df["ts"].astype(float)
        return df

    def crawl_workspace(self):
        if self.CHANNELS == None or self.USERS == None:
            self.init_scraper()

        channel_dfs = []
        channels_list = self.CHANNELS.keys()
        for channel in channels_list:
            try:
                df = self._get_channel_messages(channel)
                channel_dfs.append(df)
            except Exception as e:
                print(e)

        return {
            "channel_metadata": self.CHANNELS,
            "user_metadata": self.USERS,
        }, pd.concat(channel_dfs)


if __name__ == "__main__":
    slack_bot_token = "xoxb-2370583994869-2413498212784-UBMvJ1ozsR1DEniG3uw8oz8C"
    config = SlackGetterConfig(token_id=slack_bot_token)
    slack_getter = SlackGetter(config)
    metadata, df = slack_getter.crawl_workspace()
    # df = slack_getter._get_channel_messages("C02C3Q6UENQ")
    # slack_getter._get_threaded_message("C02C3Q6UENQ", int(float("1629552526.007700")))
    y = 1
