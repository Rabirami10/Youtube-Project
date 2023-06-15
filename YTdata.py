from googleapiclient.discovery import build
import pandas as pd


def youtube():
    api_key = 'AIzaSyAKxjHsvUYyWi2qDjXbuDjN_noEtAhOxuQ'
    api_service_name = "youtube"
    api_version = "v3"
    yt = build(api_service_name, api_version, developerKey=api_key)
    return yt


def get_channel_info(channel_id):  # To get channel details
    channel_info = youtube().channels().list(part='snippet,contentDetails,statistics', id=channel_id).execute()

    data = dict(Channel_id=channel_info['items'][0]['id'],
                channel_name=channel_info['items'][0]['snippet']['title'],
                Description=channel_info['items'][0]['snippet']['description'],
                Total_views=channel_info['items'][0]['statistics']['viewCount'],
                subscriber_count=channel_info['items'][0]['statistics']['subscriberCount'],
                Total_videos=channel_info['items'][0]['statistics']['videoCount'],
                Playlist_id=channel_info['items'][0]['contentDetails']['relatedPlaylists']['uploads']
                )
    return data


def get_video_id(ch):  # to get video ids
    ply_id = ch['Playlist_id']
    videos = []  # Create an empty list to store all the video ids
    next_page_token = None
    while 1:
        plylist_res = youtube().playlistItems().list(part="snippet",
                                                     playlistId=ply_id,
                                                     pageToken=next_page_token).execute()
        videos += plylist_res['items']
        next_page_token = plylist_res.get('nextPageToken')

        if next_page_token is None:
            break
    video_ids = []
    for vid in videos:
        id = vid['snippet']['resourceId']['videoId']
        video_ids.append(id)
    return video_ids


def get_video_details(video_ids):  # getting video details
    video_details = []  # Create an empty list to store all the video details

    for video_id in video_ids:
        video_response = youtube().videos().list(
            part='snippet,statistics,contentDetails',
            id=video_id
        ).execute()

        vd_data = {
            'video_id': video_response['items'][0]['id'],
            'video_Title': video_response['items'][0]['snippet']['title'],
            'video_description': video_response['items'][0]['snippet']['description'],
            'published_At': video_response['items'][0]['snippet']['publishedAt'],
            'view_count': video_response['items'][0]['statistics']['viewCount'],
            'like_count': video_response['items'][0]['statistics']['likeCount'],
            'favorite_Count': video_response['items'][0]['statistics']['favoriteCount'],
            'comment_count': video_response['items'][0]['statistics']['commentCount'],
            'duration': video_response['items'][0]['contentDetails']['duration'],
            'thumbnail': video_response['items'][0]['snippet']['thumbnails']['default']['url'],
            'caption': video_response['items'][0]['contentDetails']['caption'],
            'channel_id': video_response['items'][0]['snippet']['channelId']
        }

        video_details.append(vd_data)
    return video_details


def get_comment_details(video_ids):  # getting comments details
    all_comments = []  # Create an empty list to store all the comment details

    for video_id in video_ids:
        comment_response = youtube().commentThreads().list(
            part='snippet',
            videoId=video_id
        ).execute()

        # Iterate over each comment in the response and extract the details
        for item in comment_response['items']:
            comment_id = item['id']
            vid_id = item['snippet']['videoId']
            comment_text = item['snippet']['topLevelComment']['snippet']['textDisplay']
            author_name = item['snippet']['topLevelComment']['snippet']['authorDisplayName']
            published_at = item['snippet']['topLevelComment']['snippet']['publishedAt']

            # Create a dictionary for the comment details and append it to the list
            comment_details = {
                'comment_id': comment_id,
                'video_id': vid_id,
                'comment_text': comment_text,
                'author_name': author_name,
                'published_at': published_at
            }
            all_comments.append(comment_details)

    return all_comments


def merge_data(channel_data, video_details, comment_details):  # merging all data
    final_data = {'Channel_Details': channel_data,
                  'Video_Details': video_details,
                  "Comment_Details": comment_details
                  }
    return final_data


def format_duration(duration):  # to format duration column in video details
    duration = pd.to_timedelta(duration)
    hours = duration.seconds // 3600
    minutes = (duration.seconds // 60) % 60
    seconds = duration.seconds % 60
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
