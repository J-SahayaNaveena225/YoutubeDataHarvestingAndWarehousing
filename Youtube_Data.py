import googleapiclient.discovery
import mysql.connector
from mysql.connector import Error
import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu

# API Connection
def api_connect():
    api_key = "AIzaSyAHQbsghFZnFCaWnbzBsiWIIiD95dXCT48"
    api_service_name = "youtube"
    api_version = "v3"
    youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)
    return youtube

youtube = api_connect()

# Database Connection
def create_db_connection():
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="root",
            database="Youtubedataharvestingandwarehousing",
            port="3307"
        )
        if connection.is_connected():
            print("MySQL Database connection successful")
        return connection
    except Error as err:
        st.error(f"Error: '{err}'")
        return None

def execute_query(connection, query, values=None):
    cursor = connection.cursor()
    try:
        if values:
            cursor.execute(query, values)
        else:
            cursor.execute(query)
        connection.commit()
        print("Query executed successfully")
    except Error as err:
        st.error(f"Error: '{err}'")
    finally:
        cursor.close()

# YouTube Data Retrieval Functions
def get_channel_data(channel_id):
    try:
        request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id
        )
        response = request.execute()
        
        if not response['items']:
            st.error("No channel found with the given ID.")
            return None
        
        item = response['items'][0]
        data = {
            'channel_name': item['snippet']['title'],
            'channel_id': item['id'],
            'channel_subc': item['statistics']['subscriberCount'],
            'channel_vc': item['statistics']['viewCount'],
            'total_videos': item['statistics']['videoCount'],
            'channel_des': item['snippet']['description'],
            'channel_plid': item['contentDetails']['relatedPlaylists']['uploads']
        }
        return data
    except Exception as e:
        st.error(f"An error occurred while fetching channel data: {str(e)}")
        return None

def get_video_ids(channel_id):
    video_ids = []
    try:
        request = youtube.channels().list(
            part="contentDetails",
            id=channel_id
        )
        response = request.execute()
        
        if not response['items']:
            st.error("No channel found with the given ID.")
            return []
        
        playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        next_page_token = None
        
        while True:
            request = youtube.playlistItems().list(
                part="snippet",
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token
            )
            response = request.execute()
            
            video_ids.extend([item['snippet']['resourceId']['videoId'] for item in response['items']])
            
            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break
        
        return video_ids
    except Exception as e:
        st.error(f"An error occurred while fetching video IDs: {str(e)}")
        return []

def get_video_data(video_ids):
    video_data = []
    try:
        for i in range(0, len(video_ids), 50):
            request = youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id=','.join(video_ids[i:i+50])
            )
            response = request.execute()
            
            for item in response['items']:
                data = {
                    'Channel_name': item['snippet']['channelTitle'],
                    'Channel_Id': item['snippet']['channelId'],
                    'Video_Id': item['id'],
                    'Title': item['snippet']['title'],
                    'Tags': ','.join(item['snippet'].get('tags', [])),
                    'Thumbnail': item['snippet']['thumbnails']['default']['url'],
                    'Description': item['snippet'].get('description', ''),
                    'Published_Date': item['snippet']['publishedAt'],
                    'Duration': item['contentDetails']['duration'],
                    'Views': item['statistics'].get('viewCount', 0),
                    'Likes': item['statistics'].get('likeCount', 0),
                    'Comments': item['statistics'].get('commentCount', 0),
                    'Favorite_Count': item['statistics']['favoriteCount'],
                    'Definition': item['contentDetails']['definition'],
                    'Caption_Status': item['contentDetails']['caption']
                }
                video_data.append(data)
        return video_data
    except Exception as e:
        st.error(f"An error occurred while fetching video data: {str(e)}")
        return []

def get_playlist_details(channel_id):
    try:
        playlists = []
        next_page_token = None
        while True:
            request = youtube.playlists().list(
                part="snippet,contentDetails",
                channelId=channel_id,
                maxResults=50,
                pageToken=next_page_token
            )
            response = request.execute()
            
            for item in response['items']:
                data = {
                    "Playlist_Id": item['id'],
                    "Title": item['snippet']['title'],
                    "Channel_Id": item['snippet']["channelId"],
                    "Channel_Name": item['snippet']['channelTitle'],
                    "Published_Date": item['snippet']['publishedAt'],
                    "Video_Count": item['contentDetails']['itemCount']
                }
                playlists.append(data)
            
            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break
        
        return playlists
    except Exception as e:
        st.error(f"An error occurred while fetching playlist details: {str(e)}")
        return []

def get_comment_data(video_ids):
    comment_data = []
    try:
        for video_id in video_ids:
            try:
                request = youtube.commentThreads().list(
                    part="snippet",
                    videoId=video_id,
                    maxResults=100
                )
                response = request.execute()
                
                for item in response['items']:
                    data = {
                        "Comment_Id": item['snippet']['topLevelComment']['id'],
                        "Video_Id": item['snippet']['topLevelComment']['snippet']['videoId'],
                        "Comment_Text": item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        "Comment_Author": item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        "Comment_Published": item['snippet']['topLevelComment']['snippet']['publishedAt']
                    }
                    comment_data.append(data)
            except Exception as e:
                print(f"Error fetching comments for video {video_id}: {str(e)}")
                continue
        
        return comment_data
    except Exception as e:
        st.error(f"An error occurred while fetching comment data: {str(e)}")
        return []

# Database Operations
def create_tables(connection):
    create_channels_table = '''
    CREATE TABLE IF NOT EXISTS channels (
        channel_name VARCHAR(100),
        channel_id VARCHAR(80) PRIMARY KEY,
        channel_subc BIGINT,
        channel_vc BIGINT,
        total_videos BIGINT,
        channel_des TEXT,
        channel_plid VARCHAR(80)
    )
    '''
    
    create_videos_table = '''
    CREATE TABLE IF NOT EXISTS videos (
        Channel_name VARCHAR(100),
        Channel_Id VARCHAR(100),
        Video_Id VARCHAR(30) PRIMARY KEY,
        Title VARCHAR(150),
        Thumbnail VARCHAR(200),
        Description TEXT,
        Published_Date VARCHAR(50),
        Duration VARCHAR(20),
        Views BIGINT,
        Likes BIGINT,
        Comments INT,
        Favorite_Count INT,
        Definition VARCHAR(10),
        Caption_Status VARCHAR(50)
    )
    '''
    
    create_playlists_table = '''
    CREATE TABLE IF NOT EXISTS playlists (
        Playlist_Id VARCHAR(100) PRIMARY KEY,
        Title VARCHAR(150),
        Channel_Id VARCHAR(100),
        Channel_Name VARCHAR(100),
        Published_Date VARCHAR(50),
        Video_Count INT
    )
    '''
    
    create_comments_table = '''
    CREATE TABLE IF NOT EXISTS comments (
        Comment_Id VARCHAR(100) PRIMARY KEY,
        Video_Id VARCHAR(50),
        Comment_Text TEXT,
        Comment_Author VARCHAR(150),
        Comment_Published VARCHAR(50)
    )
    '''
    
    tables = [create_channels_table, create_videos_table, create_playlists_table, create_comments_table]
    
    for table in tables:
        execute_query(connection, table)

def insert_into_channels(connection, channel_data):
    query = '''
    INSERT INTO channels (channel_name, channel_id, channel_subc, channel_vc, total_videos, channel_des, channel_plid)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
    channel_name = VALUES(channel_name),
    channel_subc = VALUES(channel_subc),
    channel_vc = VALUES(channel_vc),
    total_videos = VALUES(total_videos),
    channel_des = VALUES(channel_des),
    channel_plid = VALUES(channel_plid)
    '''
    values = tuple(channel_data.values())
    execute_query(connection, query, values)

def insert_into_videos(connection, video_data):
    query = '''
    INSERT INTO videos (Channel_name, Channel_Id, Video_Id, Title, Thumbnail, Description, 
                        Published_Date, Duration, Views, Likes, Comments, Favorite_Count, 
                        Definition, Caption_Status)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
    Channel_name = VALUES(Channel_name),
    Channel_Id = VALUES(Channel_Id),
    Title = VALUES(Title),
    Thumbnail = VALUES(Thumbnail),
    Description = VALUES(Description),
    Published_Date = VALUES(Published_Date),
    Duration = VALUES(Duration),
    Views = VALUES(Views),
    Likes = VALUES(Likes),
    Comments = VALUES(Comments),
    Favorite_Count = VALUES(Favorite_Count),
    Definition = VALUES(Definition),
    Caption_Status = VALUES(Caption_Status)
    '''
    values = []
    for video in video_data:
        values.append((
            video['Channel_name'],
            video['Channel_Id'],
            video['Video_Id'],
            video['Title'],
            video['Thumbnail'],
            video['Description'],
            video['Published_Date'],
            video['Duration'],
            video['Views'],
            video['Likes'],
            video['Comments'],
            video['Favorite_Count'],
            video['Definition'],
            video['Caption_Status']
        ))
    
    cursor = connection.cursor()
    try:
        cursor.executemany(query, values)
        connection.commit()
        st.success(f"Successfully inserted {len(values)} video records.")
    except mysql.connector.Error as err:
        st.error(f"Error inserting video data: {err}")
        connection.rollback()
    finally:
        cursor.close()

def insert_into_playlists(connection, playlist_data):
    query = '''
    INSERT INTO playlists (Playlist_Id, Title, Channel_Id, Channel_Name, Published_Date, Video_Count)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
    Title = VALUES(Title),
    Channel_Id = VALUES(Channel_Id),
    Channel_Name = VALUES(Channel_Name),
    Published_Date = VALUES(Published_Date),
    Video_Count = VALUES(Video_Count)
    '''
    values = [tuple(playlist.values()) for playlist in playlist_data]
    cursor = connection.cursor()
    cursor.executemany(query, values)
    connection.commit()
    cursor.close()

def insert_into_comments(connection, comment_data):
    query = '''
    INSERT INTO comments (Comment_Id, Video_Id, Comment_Text, Comment_Author, Comment_Published)
    VALUES (%s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
    Video_Id = VALUES(Video_Id),
    Comment_Text = VALUES(Comment_Text),
    Comment_Author = VALUES(Comment_Author),
    Comment_Published = VALUES(Comment_Published)
    '''
    values = [tuple(comment.values()) for comment in comment_data]
    cursor = connection.cursor()
    cursor.executemany(query, values)
    connection.commit()
    cursor.close()

def is_channel_exists(connection, channel_id):
    query = "SELECT * FROM channels WHERE channel_id = %s"
    cursor = connection.cursor(buffered=True)
    cursor.execute(query, (channel_id,))
    result = cursor.fetchone()
    cursor.close()
    return result is not None

# Streamlit App
st.set_page_config(page_title="YouTube Data Harvesting and Warehousing", page_icon=":movie_camera:")

with st.sidebar:
    opt = option_menu("Menu", ['Home', 'Fetch & Store', 'Q/A'])

if opt == "Home":
    st.title(":red[YouTube Data Harvesting and Warehousing]")
    st.write("#")
    st.write("This project gathers data from YouTube channels using the YouTube Data API, followed by meticulous processing and subsequent warehousing.")
    st.write("To use this application, enter a YouTube channel ID in the 'Fetch & Store' section to obtain channel information via the YouTube Data API.")
    st.write("A SQL data warehouse will host the collected data.")
    st.write("The 'Q/A' section provides several search possibilities for retrieving data from the SQL database.")

elif opt == "Fetch & Store":
    st.markdown("#")
    st.write("### Enter the YouTube Channel ID")
    channel_id = st.text_input("Enter here:")
    
    if st.button('Fetch & Store'):
        connection = create_db_connection()
        if connection:
            if is_channel_exists(connection, channel_id):
                st.error(f"Channel with ID {channel_id} already exists in the database. Please enter a new channel ID.")
            else:
                create_tables(connection)
                
                with st.spinner("Fetching data from YouTube API..."):
                    channel_data = get_channel_data(channel_id)
                    if channel_data:
                        video_ids = get_video_ids(channel_id)
                        video_data = get_video_data(video_ids)
                        playlist_data = get_playlist_details(channel_id)
                        comment_data = get_comment_data(video_ids[:5])  # Limit to first 5 videos to avoid rate limiting
                
                if channel_data:
                    with st.spinner("Storing data in the database..."):
                        insert_into_channels(connection, channel_data)
                        insert_into_videos(connection, video_data)
                        insert_into_playlists(connection, playlist_data)
                        insert_into_comments(connection, comment_data)
                    
                    st.success("Data fetched and stored successfully!")
                else:
                    st.error("Failed to fetch channel data. Please check the channel ID and try again.")
            
            connection.close()
        else:
            st.error("Failed to connect to the database. Please check your connection settings.")

elif opt == "Q/A":
    questions = [
        "1. What are the names of all the videos and their corresponding channels?",
        "2. Which channels have the most number of videos, and how many videos do they have?",
        "3. What are the top 10 most viewed videos and their respective channels?",
        "4. How many comments were made on each video, and what are their corresponding video names?",
        "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
        "6. What is the total number of likes for each video, and what are their corresponding video names?",
        "7. What is the total number of views for each channel, and what are their corresponding channel names?",
        "8. What are the names of all the channels that have published videos in the year 2022?",
        "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
        "10. Which videos have the highest number of comments, and what are their corresponding channel names?"
    ]
    
    question = st.selectbox("Select your question", questions)
    
    if st.button('Execute Query'):
        connection = create_db_connection()
        if connection:
            cursor = connection.cursor()
            
            if question == questions[0]:
                query = "SELECT Title, Channel_name FROM videos"
                cursor.execute(query)
                result = cursor.fetchall()
                df = pd.DataFrame(result, columns=["Video Title", "Channel Name"])
                st.write(df)
            
            elif question == questions[1]:
                query = "SELECT Channel_name, COUNT(Video_Id) as video_count FROM videos GROUP BY Channel_name ORDER BY video_count DESC"
                cursor.execute(query)
                result = cursor.fetchall()
                df = pd.DataFrame(result, columns=["Channel Name", "Video Count"])
                st.write(df)
            
            elif question == questions[2]:
                query = "SELECT Title, Channel_name, Views FROM videos ORDER BY Views DESC LIMIT 10"
                cursor.execute(query)
                result = cursor.fetchall()
                df = pd.DataFrame(result, columns=["Video Title", "Channel Name", "Views"])
                st.write(df)
            
            elif question == questions[3]:
                query = "SELECT Title, Comments FROM videos ORDER BY Comments DESC"
                cursor.execute(query)
                result = cursor.fetchall()
                df = pd.DataFrame(result, columns=["Video Title", "Comment Count"])
                st.write(df)
            
            elif question == questions[4]:
                query = "SELECT Title, Channel_name, Likes FROM videos ORDER BY Likes DESC LIMIT 10"
                cursor.execute(query)
                result = cursor.fetchall()
                df = pd.DataFrame(result, columns=["Video Title", "Channel Name", "Likes"])
                st.write(df)
            
            elif question == questions[5]:
                query = "SELECT Title, Likes FROM videos ORDER BY Likes DESC"
                cursor.execute(query)
                result = cursor.fetchall()
                df = pd.DataFrame(result, columns=["Video Title", "Likes"])
                st.write(df)
            
            elif question == questions[6]:
                query = "SELECT Channel_name, SUM(Views) as Total_Views FROM videos GROUP BY Channel_name ORDER BY Total_Views DESC"
                cursor.execute(query)
                result = cursor.fetchall()
                df = pd.DataFrame(result, columns=["Channel Name", "Total Views"])
                st.write(df)
            
            elif question == questions[7]:
                query = "SELECT DISTINCT Channel_name FROM videos WHERE YEAR(STR_TO_DATE(Published_Date, '%Y-%m-%dT%H:%i:%sZ')) = 2022"
                cursor.execute(query)
                result = cursor.fetchall()
                df = pd.DataFrame(result, columns=["Channel Name"])
                st.write(df)
            
            elif question == questions[8]:
                query = """
                SELECT Channel_name, 
                       AVG(TIME_TO_SEC(STR_TO_DATE(Duration, '%H:%i:%s'))) as Avg_Duration_Seconds
                FROM videos
                GROUP BY Channel_name
                ORDER BY Avg_Duration_Seconds DESC
                """
                cursor.execute(query)
                result = cursor.fetchall()
                df = pd.DataFrame(result, columns=["Channel Name", "Average Duration (seconds)"])
                df["Average Duration (seconds)"] = df["Average Duration (seconds)"].apply(lambda x: f"{x:.2f}")
                st.write(df)
            
            elif question == questions[9]:
                query = "SELECT Title, Channel_name, Comments FROM videos ORDER BY Comments DESC LIMIT 10"
                cursor.execute(query)
                result = cursor.fetchall()
                df = pd.DataFrame(result, columns=["Video Title", "Channel Name", "Comment Count"])
                st.write(df)
            
            cursor.close()
            connection.close()
        else:
            st.error("Failed to connect to the database. Please check your connection settings.")

if __name__ == "__main__":
    st.sidebar.title("About")
    st.sidebar.info(
        "This Streamlit app is designed for YouTube data harvesting and warehousing. "
        "It allows users to fetch data from YouTube channels, store it in a MySQL database, "
        "and perform various analyses on the collected data."
    )
    st.sidebar.title("Navigation")
    st.sidebar.info(
        "Use the menu above to navigate between the different sections of the app:\n"
        "- Home: Overview of the application\n"
        "- Fetch & Store: Retrieve and store data from YouTube\n"
        "- Q/A: Perform analyses on the stored data"
    )