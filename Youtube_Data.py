import googleapiclient.discovery
import mysql.connector
import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu
import time

##API KEY Connection
def Api_connect():
    Api_Id = "AIzaSyAHQbsghFZnFCaWnbzBsiWIIiD95dXCT48"

    api_service_name = "youtube"
    api_version = "v3"

    youtube = googleapiclient.discovery.build( api_service_name, api_version,developerKey=Api_Id)

    return youtube
youtube=Api_connect()

##get channel details
def get_channel_data(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()
    for i in response['items']:
         data={
            'channel_name':i['snippet']['title'],
            'channel_id':i['id'],
            'channel_subc':i['statistics']['subscriberCount'],
            'channel_vc':i['statistics']['viewCount'],
            'total_videos':i['statistics']['videoCount'],
            'channel_des':i['snippet']['description'],
            'channel_plid':i['contentDetails']['relatedPlaylists']['uploads']
    }
    return data

##get video_ids
def get_videos_ids(channel_id):
    video_ids = []
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()

    Playlist_Id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None

    while True:
        response1 = youtube.playlistItems().list(
            part="snippet",
            playlistId=Playlist_Id,
            pageToken=next_page_token
        ).execute()
        for i in range(len(response1['items'])):
          video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])

        next_page_token=response1.get('nextPageToken')
        if next_page_token is None:
          break

    return video_ids

##get video_data
def get_video_data(channel_id):
    video_data=[]
    for video_id in video_ids:
        request=youtube.videos().list(
           part='snippet,ContentDetails,statistics', # Changed 'CondentDetails' to 'ContentDetails'
           id=video_id
        )
        response=request.execute()

        for item in response['items']:
            data={'Channel_name':item['snippet']['channelTitle'],
                  'Channel_Id':item['snippet']['channelId'],
                  'Video_Id':item['id'],
                  'Title':item['snippet']['title'],
                  'Tags':item['snippet'].get('tags'),
                  'Thumbnail':item['snippet']['thumbnails']['default']['url'],
                  'Description':item['snippet'].get('description'),
                  'Published_Date':item['snippet']['publishedAt'],
                  'Duration':item['contentDetails']['duration'],
                  'Views':item['statistics'].get('viewCount'),
                  'likes':item['statistics'].get('likeCount'),
                  'Comments':item['statistics'].get('commentCount'),
                  'Favorite_Count':item['statistics']['favoriteCount'],
                  'Definition':item['contentDetails']['definition'],
                  'Caption_Status':item['contentDetails']['caption']
                }
            video_data.append(data)
    return video_data


##get_playlist_details

def get_playlist_details(channel_id):

      next_page_token=None
      All_data=[]
      while True:
            request=youtube.playlists().list(
                  part="snippet,contentDetails",
                  channelId=channel_id,
                  maxResults=50,
                  pageToken=next_page_token
                  )
            response=request.execute()

            for item in response['items']:
                  data={"Playlist_Id":item['id'],
                        "Title":item[ 'snippet'][ 'title'],
                        "Channel_Id":item[ 'snippet']["channelId"],
                        "Chnanel_Name":item[ 'snippet']['channelTitle'],
                        "Published_Date":item[ 'snippet']['publishedAt'],
                        "Video_count":item[ 'contentDetails']['itemCount']}
                  All_data.append(data)

            next_page_token=response.get('nextPageToken')
            if next_page_token is None:
                  break   
      return All_data  


##get comment_data 
def get_comment_data(video_ids):
    Comment_data=[]
    try: 
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response=request.execute()

            for item in response['items']:
                data={"Comment_Id":item['snippet']['topLevelComment']['id'],
                    "Video_Id":item['snippet']['topLevelComment'][ 'snippet']['videoId'],
                    "Comment_Text":item['snippet']['topLevelComment'][ 'snippet']['textDisplay'],
                    "Comment_Author":item['snippet']['topLevelComment'][ 'snippet']['authorDisplayName'],
                    "Comment_Publishes":item['snippet']['topLevelComment'][ 'snippet']['publishedAt']}
                Comment_data.append(data)
                


    except:
        pass  
    
    return Comment_data

      
###mysql connection

mydb=mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="Youtubedataharvestingandwarehousing",
    port="3307")

mycursor = mydb.cursor()


def channels_table(channel_details):

    mydb=mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="Youtubedataharvestingandwarehousing",
    port="3307")

    mycursor = mydb.cursor()
    mycursor.execute
    try:
        create_query='''create table if not exists channels(channel_name varchar(100),
                                                                channel_id varchar(80) primary key,
                                                                channel_subc bigint,
                                                                channel_vc bigint,
                                                                total_videos bigint,
                                                                channel_des text,
                                                                channel_plid varchar(80))'''
            
        mycursor.execute(create_query)
        print("table created")
        mydb.commit()
            
    except:
        print("Channels table already created")

    
    sql = "INSERT INTO channels(channel_name,channel_id,channel_subc,channel_vc,total_videos,channel_des, channel_plid)VALUES (%s,%s,%s,%s,%s,%s,%s)"
    val = tuple(channel_details.values())
    mycursor.execute(sql, val)

    mydb.commit()

    print(mycursor.rowcount, "record inserted.")

#channels_table(get_channel_data())


def videos_table(video_details):

    mydb=mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="Youtubedataharvestingandwarehousing",
    port="3307")

    mycursor = mydb.cursor()
    mycursor.execute
   ###table creation
    

    create_query='''create table if not exists videos(Channel_name varchar(100),
                                                    Channel_Id varchar(100),
                                                    Video_Id varchar(30) primary key,
                                                    Title varchar(100),
                                                    Thumbnail varchar(200),
                                                    Description text,
                                                    Published_Date varchar(200),
                                                    Duration varchar(20),
                                                    Views bigint,
                                                    likes bigint,
                                                    Comments int,
                                                    Favorite_Count int,
                                                    Definition varchar(10),
                                                    Caption_Status varchar(50))'''
    mycursor.execute(create_query)
    mydb.commit()
    for video_detail in video_details:
        try:
            sql = "INSERT INTO videos(channel_name,channel_id,Video_Id,Title,Thumbnail,Description,Published_Date,Duration,Views,likes,Comments,Favorite_Count,Definition,Caption_Status)VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            val =tuple(video_detail.values())
            mycursor.execute(sql, val)
            mydb.commit()
        except: 
            print("id already existing in the table")   
            
        mydb.commit()
        print(mycursor.rowcount, "record inserted.")

#videos_table(get_video_data(get_videos_ids()))


def playlists_table(playlist_details):

    mydb=mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="Youtubedataharvestingandwarehousing",
    port="3307")

    mycursor = mydb.cursor()
    mycursor.execute
    try:
        create_query='''create table if not exists playlists(Playlist_Id varchar(100) primary key,
                                                        Title varchar(100),
                                                        Channel_Id varchar(100),
                                                        Chnanel_Name varchar(100),
                                                        Published_Date varchar(20),
                                                        Video_count int)'''
            
        mycursor.execute(create_query)
        print("table created")
        mydb.commit()
            
    except:
        print("table already created")

    for playlist_detail in playlist_details:
        
        try:
            sql = "INSERT INTO playlists(Playlist_Id,Title,Channel_Id,Chnanel_Name,Published_Date,Video_count)VALUES (%s,%s,%s,%s,%s,%s)"
            val = tuple(playlist_detail.values())
            mycursor.execute(sql, val)
        except:
            print("id already existing in the table")
            
        mydb.commit()
        print(mycursor.rowcount, "record inserted.")

def comments_table(comments_details):

    mydb=mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="Youtubedataharvestingandwarehousing",
    port="3307")

    mycursor = mydb.cursor()
    mycursor.execute
    try:
        create_query='''create table if not exists comments(Comment_Id varchar(100) primary key,
                                                        Video_Id varchar(50),
                                                        Comment_Text text,
                                                        Comment_Author varchar(150),
                                                        Comment_Publishes varchar(20))'''
            
        mycursor.execute(create_query)
        print("table created")
        mydb.commit()
            
    except:
        print("table already created")

    for comment_detail in comments_details:
        try:

            sql = "INSERT INTO comments(Comment_Id,Video_Id,Comment_Text,Comment_Author,Comment_Publishes)VALUES (%s,%s,%s,%s,%s)"
            val = tuple(comment_detail.values())
            mycursor.execute(sql, val)
            mydb.commit()

        except:
            print("id already existing in the table")


        mydb.commit()

        print(mycursor.rowcount, "record inserted.")

def is_channel_exists(channel_id):
    try:
        mycursor.execute("select * from channels where channel_id=%s", (channel_id,))
    except mysql.connector.Error as err:
        return False
    
    result=mycursor.fetchall()
    if len(result)==0:
        return False
    else:
        return True

with st.sidebar:
     opt = option_menu("Menu",
                    ['Home','Fetch & Store','Q/A'])
     
if opt=="Home":
        st.title(''':red[_YOUTUBE DATA HARVESTING AND WAREHOUSING_]''')
        st.write("#")
        st.write("This project gathers data from YouTube channels using the YouTube Data API, followed by meticulous processing and subsequent warehousing.")
        st.write("To develop a Streamlit application that enables users to enter a YouTube channel ID and obtain channel information via the YouTube Data API.")
        st.write("A SQL data warehouse will host the collected data.")
        st.write("The application ought to provide several search possibilities for retrieving data from the SQL database.")

if opt == ("Fetch & Store"):
                
        st.markdown("#    ")
        st.write("### ENTER THE YOUTUBE CHANNEL ID ")
        channel_id = st.text_input("enter here below")
        
        if st.button('Fetch & Store'):
            st.write("Fetching data from Youtube API")
            # if is_channel_exists(channel_id):
            #     st.write("Data already fetched and stored")
            # else:
            channel_details=get_channel_data(channel_id)
            print("channel_details", channel_details)
            channels_table(channel_details)
            video_ids=get_videos_ids(channel_id)
            print("video_ids", video_ids)
            video_details=get_video_data(channel_id)
            print("video_details", video_details)
            videos_table(video_details)
            playlist_details=get_playlist_details(channel_id)
            playlists_table(playlist_details)
            comment_details=get_comment_data(video_ids)
            comments_table(comment_details)
            st.write("Data fetched and stored successfully")
            
if opt=="Q/A":
    question=st.selectbox("Select your question",("1.What are the names of all the videos and their corresponding channels?",
                                              "2.Which channels have the most number of videos, and how many videos do they have?",
                                              "3.What are the top 10 most viewed videos and their respective channels?",
                                              "4.How many comments were made on each video, and what are their corresponding video names?",
                                              "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                                              "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                              "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                                              "8.What are the names of all the channels that have published videos in the year 2022?",
                                              "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                              "10.Which videos have the highest number of comments, and what are their corresponding channel names?"))
    
    if question=="1.What are the names of all the videos and their corresponding channels?":
        mycursor.execute("select Title,Channel_name from videos")
        result=mycursor.fetchall()
        st.write(pd.DataFrame(result,columns=["Title","Channel_name"]))
        
    if question=="2.Which channels have the most number of videos, and how many videos do they have?":
        mycursor.execute("select Channel_name,count(Video_Id) from videos group by Channel_name order by count(Video_Id) desc")
        result=mycursor.fetchall()
        st.write(pd.DataFrame(result,columns=["Channel_name","Number_of_videos"]))
        
    if question=="3.What are the top 10 most viewed videos and their respective channels?":
        mycursor.execute("select Title,Channel_name,Views from videos order by Views desc limit 10")
        result=mycursor.fetchall()
        st.write(pd.DataFrame(result,columns=["Title","Channel_name","Views"]))
        
    if question=="4.How many comments were made on each video, and what are their corresponding video names?":
        mycursor.execute("select Title,Comments from videos")
        result=mycursor.fetchall()
        st.write(pd.DataFrame(result,columns=["Title","Comments"]))
        
    if question=="5.Which videos have the highest number of likes, and what are their corresponding channel names?":
        mycursor.execute("select Title,Channel_name,likes from videos order by likes desc")
        result=mycursor.fetchall()
        st.write(pd.DataFrame(result,columns=["Title","Channel_name","likes"]))
        
    if question=="6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
        mycursor.execute("select Title,likes,Comments from videos")
        result=mycursor.fetchall()
        st.write(pd.DataFrame(result,columns=["Title","likes","Comments"]))
        
    if question=="7.What is the total number of views for each channel, and what are their corresponding channel names?":
        mycursor.execute("select Channel_name,sum(Views) from videos group by Channel_name")
        result=mycursor.fetchall()
        st.write(pd.DataFrame(result,columns=["Channel_name","Total_views"]))
        
    if question=="8.What are the names of all the channels that have published videos in the year 2022?":
        mycursor.execute("select distinct Channel_name from videos where Published_Date like '%2022%'")
        result=mycursor.fetchall()
        st.write(pd.DataFrame(result,columns=["Channel_name"]))
        
    if question=="9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
        mycursor.execute("select Channel_name,avg(Duration) from videos group by Channel_name")
        result=mycursor.fetchall()
        st.write(pd.DataFrame(result,columns=["Channel_name","Average_duration"]))
        
    if question=="10.Which videos have the highest number of comments, and what are their corresponding channel names?":
        mycursor.execute("select Title,Channel_name,Comments from videos order by Comments desc")
        result=mycursor.fetchall()
        st.write(pd.DataFrame(result,columns=["Title","Channel_name","Comments"]))