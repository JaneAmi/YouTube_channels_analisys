# for work with YouTube API
from googleapiclient.discovery import build

# for data processing
import pandas as pd
import sqlite3 as sq
from dateutil import parser
import isodate
import openpyxl
from itertools import chain

def get_channel_info(youtube, channel_ids):  # collect channels data
    all_data = []
    request = youtube.channels().list(
            part="snippet,brandingSettings,statistics,topicDetails,contentDetails",
            id=','.join(channel_ids)
        )
    response = request.execute()  # 

    for item in response['items']:
        # creating 
        data = {'channelId': item['id'],
        'channelName': item['snippet']['title'],
        'customURL': item['snippet']['customUrl'],
        'channelDescribtion': item['snippet']['description'],
        'subscribers': item['statistics']['subscriberCount'],
        'views': item['statistics']['viewCount'],
        'totalVideos': item['statistics']['videoCount'],
        'playlistId': item['contentDetails']['relatedPlaylists']['uploads'],
        'topicCategories': item['topicDetails']['topicCategories']
        }

        all_data.append(data)

    return pd.DataFrame(all_data)


def get_video_ids(youtube, playlist_id):  # get all video ids for channel
    video_ids = []
    request = youtube.playlistItems().list(
        part="snippet, contentDetails",
        playlistId=playlist_id,
        maxResults = 50
    )
    response = request.execute()

    for item in response['items']:
        video_ids.append(item['contentDetails']['videoId'])

    next_page_token = response.get('nextPageToken')

    while next_page_token is not None:
        request = youtube.playlistItems().list(
        part="snippet, contentDetails",
        playlistId=playlist_id,
        maxResults = 50,
        pageToken = next_page_token
        )
        response = request.execute()

        for item in response['items']:
            video_ids.append(item['contentDetails']['videoId'])

        next_page_token = response.get('nextPageToken')

    return video_ids


def full_list_of_videos(youtube, channel_info): # create combained list of all videos of gven channels
    result = list(chain.from_iterable(
        [get_video_ids(youtube, x) for x in channel_info['playlistId'].tolist()]
        ))
    return result


def get_video_details(youtube, video_ids):  # get videos data
    all_video_info = []

    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=','.join(video_ids[i:i+50])
        )
        response = request.execute()

        for video in response['items']:
            stats_to_keep = {'snippet': ['channelId', 'title', 'description', 'tags', 'publishedAt'],
            'statistics': ['viewCount', 'likeCount', 'commentCount'],
            'contentDetails': ['duration']}
            video_info = {}
            video_info['video_id'] = video['id']

            for k in stats_to_keep.keys():
                for v in stats_to_keep[k]:
                    try:
                        video_info[v] = video[k][v]
                    except:
                        video_info[v] = None

            all_video_info.append(video_info)
        result = pd.DataFrame(all_video_info)
    return result


def miss_data_cnames(*args):  # check missing data in given tables
    for t in args:
        if not t.isna().sum().any():
            print(f'There is not missing date in table "{t.name}"')
        else:
            for i, v in dict(t.isna().sum()).items():
                if v != 0:
                    print(f'Table "{t.name}" has {v} null values in column "{i}"')


def normalise_row(row, pr_channels):  # checking if channel is propaganda or liberal
    if row['channelId'] in pr_channels:
        return 'propaganda'
    else:
        return 'liberal'