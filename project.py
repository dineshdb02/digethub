import streamlit as st
import pandas as pd
import pymongo
import psycopg2



client=pymongo.MongoClient('mongodb+srv://dineshbabu02:Newbornbaby02@cluster0.chydtsj.mongodb.net/?retryWrites=true&w=majority')

# initialize google youtube api
import googleapiclient.discovery
def api_key():
  api_id='AIzaSyC2rUtAyTxOb7Q4BdDUYSu8D93l34GODck'
  api_service_name = "youtube"
  api_version = "v3"
  youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_id)

  return youtube


# get channel details
def channel_details(channel_id):

  request = youtube.channels().list(part="snippet,contentDetails,statistics",id=channel_id)
  response = request.execute()
  
  for i in response['items']:
   channel_info=({'channel_name ': i['snippet']['title'],
        'channel_id':channel_id,
        'subscribers ': i['statistics']['subscriberCount'],
        'channel_views': i['statistics']['viewCount'],
        'Description ': i['snippet']['description'],
        'Playlist_id': i['contentDetails']['relatedPlaylists']['uploads']

        })
  return channel_info

# get list of playlist ids and info
def fetch_playdetails(channel_id):
  request = youtube.playlists().list(
          part="snippet,contentDetails",
          channelId=channel_id,
          maxResults=25
      )
  response = request.execute()
  data=[]
  for i in response['items']:
    data.append({'playlist_id':i['id'],
          'title':i['snippet']['title'],
          'channelId':i['snippet']['channelId'],
          'channel_name':i['snippet']['channelTitle'],
          'published_at':i['snippet']['publishedAt'],
          'videocount':i['contentDetails']['itemCount']
          })
  return data   # print(i)

# created for skip to check nextpagetoken and do first iteration  
count = 0

# get playlist data on per page depends on pageToken
def play_details(playlist_id, next_page_token):

  request=youtube.playlistItems().list(
        part="snippet,contentDetails",
        maxResults=50,
        playlistId=playlist_id,
        pageToken= next_page_token
    )
  response = request.execute()
  return response

# store all the playlist data with video info and ids
playlist_det = []

# get all the info for playlists
def fetch_all_playlist_data(playlist):
  global count
  for i in range(len(playlist)):
    count=0
    fetch_details(playlist[i]['playlist_id'], None)

# get all the info for per playlist
def fetch_details(playlist_id, next_page_token):
  global count
  if next_page_token or count == 0 :
    perPageDetails = play_details(playlist_id,next_page_token)
    playlist_det.append(perPageDetails)
    count+=1
    if 'nextPageToken' in perPageDetails.keys():
      fetch_details(playlist_id,perPageDetails['nextPageToken'])

# get all video comments info through video id
def fetch_video_comments(video_id):
  request = youtube.commentThreads().list(
        part="snippet,replies",
        videoId=video_id
    )
  try:
    response = request.execute()
  except:
      response = None

  return response

# get all video info through video id
def fetch_video_data(video_id):
  request = youtube.videos().list(
        part="snippet,contentDetails,statistics",
        id=video_id
    )
  response = request.execute()
  return response
video_data=[]
comment_data=[]
video_count=0

# get video info and comment info and return expected structure
def fetch_videos_details(playlist_datas):

  global video_count
  for i in range(len(playlist_datas)):
    for j in range(len(playlist_datas[i]['items'])):
      vid_id=playlist_datas[i]['items'][j]['snippet']['resourceId']['videoId']
      vid_dt=fetch_video_data(vid_id)
      vid_cmt=fetch_video_comments(vid_id)
      video_count+=1
      cmt={}
      if(vid_cmt is not None and len(vid_cmt['items']) > 0 and 'snippet' in (vid_cmt['items'][0]).keys()):
         for ij in range(len(vid_cmt['items'])):
               cmt['Comment_Id_'+str(ij + 1)] = {
                "Comment_Id": vid_cmt['items'][ij]['id'] if 'id' in (vid_cmt['items'][ij]).keys() else None,
                "channel_Id":vid_cmt['items'][ij]['snippet']["channelId"],
                "Comment_Text": vid_cmt['items'][ij]['snippet']['topLevelComment']['snippet']['textDisplay'] if 'textDisplay' in (vid_cmt['items'][ij]['snippet']['topLevelComment']['snippet']).keys() else None,
                "Comment_Author":vid_cmt['items'][ij]['snippet']['topLevelComment']['snippet']['authorDisplayName']  if 'authorDisplayName' in (vid_cmt['items'][ij]['snippet']['topLevelComment']['snippet']).keys() else None,
                "Comment_PublishedAt": vid_cmt['items'][ij]['snippet']['topLevelComment']['snippet']['publishedAt'] if 'publishedAt' in (vid_cmt['items'][ij]['snippet']['topLevelComment']['snippet']).keys() else None
               }
             

      
      video_data.append(
        {"Video_Id_"+str(video_count): {
        "Video_Id":  vid_id,
        "Channelid": len(vid_dt['items']) > 0 and vid_dt['items'][0]['snippet']['channelId'] or None,
        "Video_Name": len(vid_dt['items']) > 0 and vid_dt['items'][0]['snippet']['title'] or None,
        "Video_Description":len(vid_dt['items']) > 0 and vid_dt['items'][0]['snippet']['description'] or None,
        "Tags":len(vid_dt['items']) > 0 and 'tags' in (vid_dt['items'][0]['snippet']).keys() and vid_dt['items'][0]['snippet']['tags'] or None  ,
        "PublishedAt":len(vid_dt['items']) > 0 and vid_dt['items'][0]['snippet']['publishedAt'] or None,
        "View_Count":len(vid_dt['items']) > 0 and vid_dt['items'][0]['statistics']['viewCount'] or None,
        "Like_Count": len(vid_dt['items']) > 0 and vid_dt['items'][0]['statistics']['likeCount'] or None,
        "Favorite_Count":len(vid_dt['items']) > 0 and vid_dt['items'][0]['statistics']['favoriteCount'] or None,
        "Comment_Count":len(vid_dt['items']) > 0 and 'commentCount' in (vid_dt['items'][0]['statistics']).keys() and vid_dt['items'][0]['statistics']['commentCount']or None,
        "Duration":len(vid_dt['items']) > 0 and vid_dt['items'][0]['contentDetails']['duration'] or None,
        "Thumbnail":len(vid_dt['items']) > 0 and vid_dt['items'][0]['snippet']['thumbnails']['default']['url'] or None,
        "Caption_Status": "Available",
        # "Comments": cmt

        }}
        )
      comment_data.append(cmt)

# function which initialize and execute all the functions handled in sequence 
def channel_info(channel_id):
  channeldetail=channel_details(channel_id)
  playlist_detail = fetch_playdetails(channeldetail['channel_id'])
  fetch_all_playlist_data(playlist_detail)
  vid_details = fetch_videos_details(playlist_det)
  return {'channel_info':channeldetail,'playlist_info':playlist_detail,'video_info':video_data,'comment_info':comment_data }

# executed for initialize the youtube api 
youtube=api_key()
# executed for initialize the fetching process



# mongoDB to Sql migration

def channel_table(channel_id):
    
    db=psycopg2.connect(host='localhost',user='postgres',password='11001100',database='dinesh',port=5432)
    access=db.cursor()   
    db.commit()


    
    access.execute('''create table if not exists channels(channel_Name varchar(100),
                                                        channel_id varchar(500) primary key,
                                                        subscribers int,
                                                        channel_views int,
                                                        description text,
                                                        playlist_id varchar(100)
                                                        )''')
                                                        
    db.commit()
  
    channel_list=[]
    data=client['dinesh']
    collection=data['you']
    for channel_details in collection.find({},{"_id":0,"channel_info":1}):
        if channel_details["channel_info"]['channel_id'] == channel_id:
          channel_list.append(channel_details["channel_info"])
          
    df=pd.DataFrame(channel_list)
    df
    try:


      for index,row in df.iterrows():
        
          insert_query='''insert into channels(channel_name,
                                              channel_id,
                                              subscribers,
                                              channel_views,
                                              description,
                                              playlist_id)
                                              
                                              values(%s,%s,%s,%s,%s,%s)'''                                    
          values =(row['channel_name '],
                  row['channel_id'],
                  row['subscribers '],
                  row['channel_views'],
                  row['Description '],
                  row['Playlist_id'])
          
          access.execute(insert_query,values)
          db.commit()
              
    except Exception as e:
      st.success("Channel_Id exists")
      db.close()

# playlist table 

def playlist_table(channel_id):
    
    db=psycopg2.connect(host='localhost',user='postgres',password='11001100',database='dinesh',port=5432)
    access=db.cursor()

    db.commit()

    access.execute('''create table if not exists playlists(playlist_id varchar(100) primary key,
                                                            title varchar(500),
                                                            channelId varchar(100),
                                                            channel_name varchar(100),
                                                            published_at timestamp,
                                                            videocount int
                                                            )''')
    db.commit()
  
    play_list=[]
    data=client['dinesh']
    collection=data['you']
    for playlist_details in collection.find({},{"_id":0,"playlist_info":1}):
        for i in range(len(playlist_details['playlist_info'])):
            if(playlist_details["playlist_info"][i]['channelId'] == channel_id):
              play_list.append(playlist_details["playlist_info"][i])
    df=pd.DataFrame(play_list)
    df


    
    try:
    
       
      for index,row in df.iterrows():
    
            
            
        insert_query='''insert into playlists(playlist_id,
                                                title,
                                                channelId,
                                                channel_name,
                                                published_at,
                                                videocount)
                                                
                                                values(%s,%s,%s,%s,%s,%s)'''                                    
        values =(row['playlist_id'],
                    row['title'],
                    row['channelId'],
                    row['channel_name'],
                    row['published_at'],
                    row['videocount'])
            
        access.execute(insert_query,values)
        db.commit()
    except Exception as e:
      st.success("Playlist_Id exists")
      db.close()
 
     
              
def video_table(channel_id):

    db=psycopg2.connect(host='localhost',user='postgres',password='11001100',database='dinesh',port=5432)
    access=db.cursor()

  
    db.commit()

    
    access.execute('''create table if not exists videos(Video_Id varchar(100) primary key ,
                                                        Channelid varchar(100),
                                                        Video_Name  varchar(100),
                                                        Video_Description text,
                                                        Tags text,
                                                        PublishedAt timestamp,
                                                        View_Count bigint,
                                                        Like_Count bigint,
                                                        Favorite_Count int,
                                                        Comment_Count bigint,
                                                        Duration interval,
                                                        Thumbnail text,
                                                        Caption_Status varchar(50)
                                                            )''')
                                                            
            
        
    db.commit()
        
    



    video_list=[]
    data=client['dinesh']
    collection=data['you']
    for vid_details in collection.find({},{"_id":0,"video_info":1}):
        for i in range(len(vid_details['video_info'])):
            if (vid_details["video_info"][i]['Video_Id_'+str(i+1)]['Channelid'] == channel_id):
              video_list.append(vid_details["video_info"][i]['Video_Id_'+str(i+1)])
    df=pd.DataFrame(video_list)
    df


    try:
      
    
      for index,row in df.iterrows():
      # print(index, ":", row)
          insert_query='''insert into videos(Video_Id,
                                          Channelid,
                                          Video_Name,
                                          Video_Description,
                                          Tags,
                                          PublishedAt,
                                          View_Count,
                                          Like_Count,
                                          Favorite_Count,
                                          Comment_Count,
                                          Duration,
                                          Thumbnail,
                                          Caption_Status)
                                          
                                          values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''                                    
          values =(row['Video_Id'],
                  row['Channelid'],
                  row['Video_Name'],
                  row['Video_Description'],
                  row['Tags'],
                  row['PublishedAt'],
                  row['Like_Count'],
                  row['View_Count'],
                  row['Favorite_Count'],
                  row['Comment_Count'],
                  row['Duration'],
                  row['Thumbnail'],
                  row['Caption_Status'])
      # try:
          access.execute(insert_query,values)
          db.commit()
        
    except Exception as e:
      
      st.success("video_Id exists")
      db.close()
    
def comments_table(channel_id):


  db=psycopg2.connect(host='localhost',user='postgres',password='11001100',database='dinesh',port=5432)
  access=db.cursor()
  
  db.commit()

  
  access.execute('''create table if not exists comments(Comment_Id varchar(100),
                                                          channel_Id varchar(100),
                                                          
                                                          Comment_Text text,
                                                          Comment_Author varchar(200),
                                                          Comment_PublishedAt timestamp                                                        
                                                              )''')          
  
  db.commit()
          
  
      
      
  comment_list=[]
  data=client['dinesh']
  collection=data['you']
  cnt = 0
  for cmt_details in collection.find({'channel_info.channel_id':channel_id},{"_id":0,"comment_info":1}):
      for i in range(len(cmt_details['comment_info'])):
          for j in range(len(cmt_details['comment_info'][i])):
            if 'Comment_Id_'+str(j+1) in cmt_details["comment_info"][i].keys() and comment_list.append(cmt_details["comment_info"][i]['Comment_Id_'+str(j+1)]):
              'Comment_Id_'+str(j+1) in cmt_details["comment_info"][i].keys() and comment_list.append(cmt_details["comment_info"][i]['Comment_Id_'+str(j+1)]) or None
  
  st.success("inserted")
              
  df=pd.DataFrame(comment_list)
  df

 


  for index,row in df.iterrows():
      
      insert_query='''insert into comments(Comment_Id,
                                          channel_Id,
                                          
                                          Comment_Text,
                                          Comment_Author,
                                          Comment_PublishedAt)
                                          
                                          values(%s,%s,%s,%s,%s)'''                                    
      values =(row['Comment_Id'],
              row['channel_Id'],
              
              row['Comment_Text'],
              row['Comment_Author'],
              row['Comment_PublishedAt'])
 
      access.execute(insert_query,values)
      db.commit()
          
 


# streamlite

db=psycopg2.connect(host='localhost',user='postgres',password='11001100',database='dinesh',port=5432)
access=db.cursor()

db.commit()

    
with st.sidebar:
    st.title(":blue[ YOUTUBE DATA HARVESTING & WAREHOUSING ]")
    st.header(":red[Project]")
 
    option = st.selectbox(
    "Select Page",
    ('Collect Data', 'Migrate Data', 'Data Analysing'))
    

    st.write('You selected:', option)
    
if option == 'Collect Data':
    channel_id= st.text_input('Channel Id')


    if st.button('Insert'):
        
        chl=[]
        data=client.dinesh
        collection=data.you
       
        
        
        for chldata in collection.find({},{"_id":0,"channel_info":1}):
            
            chl.append(chldata["channel_info"]['channel_id'])
                
        if channel_id in chl:
            st.success("Channel exists")
        else:
            insert=channel_info(channel_id)
            
            inserted=collection.insert_one(insert)
            
            st.success(inserted and 'Data collected & inserted successfully')
            
def format_func(option):
    return option['channel_name ']    
            
if option == 'Migrate Data':
  channel_list=[]
  data=client['dinesh']
  collection=data['you']
  for channel_details in collection.find({},{"_id":0,"channel_info":1}):
      channel_list.append(channel_details["channel_info"])

  option = st.selectbox(
    "List of Channels",
    (channel_list),
    index=None,
    placeholder="Select Channels",
    format_func=format_func
  )

  st.write('You selected:', option is not None and option['channel_name '] or 'Selected None')
  
  if option is not None:

    options = st.multiselect(
        'Table Details',
        
        ['Channels', 'Playlists', 'videos', 'Comments'],
        
        )

    if(len(options) > 0):
      if st.button('Migrate to SQL'):
        if 'Channels' in options:
          channel_table(option['channel_id'])
          if option['channel_id'] == 'channel_id':
            st.success("Channel Inserted Successfully")
            
            
        if 'Playlists' in options:
          playlist_table(option['channel_id'])
          if option['channel_id'] == 'channel_id':
            st.success("Playlists Inserted Successfully")
            
        if 'videos' in options:
          
          video_table(option['channel_id'])
          if option['channel_id'] == 'channel_id':
            st.success("Videos Inserted Successfully")
         
        if 'Comments' in options:
          comments_table(option['channel_id'])
          if option['channel_id'] == 'channel_id':
            st.success("Comments inserted successfully")
          
          

if option == 'Data Analysing': 
    db=psycopg2.connect(host='localhost',user='postgres',password='11001100',database='dinesh',port=5432)
    access=db.cursor()

    db.commit()

    Queries = st.selectbox('Select your queries',("Select Your Queries",
                                                "1) The names of all the videos and their corresponding channels",
                                                "2) Channels have the most number of videos",
                                                "3) The top 10 most viewed videos and their respective channels",
                                                "4) Comments were made on each video and their corresponding video names",
                                                "5) Videos have the highest number of likes and their corresponding channel names",
                                                "6) The total number of likes and dislikes for each video and their corresponding video names",
                                                "7) The total number of views for each channel and their corresponding channel names",
                                                "8) The names of all the channels that have published videos in the year 2022",
                                                "9) The average duration of all videos in each channel and their corresponding channel names",
                                                "10)Videos have the highest number of comments and their corresponding channel names"))

    if Queries == "1) The names of all the videos and their corresponding channels": 
        Query1='''select video_name,channel_name from videos inner join channels on videos.channelid = channels.channel_id'''
        access.execute(Query1)
        db.commit()
        t1=access.fetchall()
        data=pd.DataFrame(t1,columns=["videoname","channelname "])
        st.write(data)
        
    elif Queries == "2) Channels have the most number of videos": 

        Query2='''select sum(videocount),channel_name from playlists group by channel_name order by max(videocount) desc limit 1'''
        access.execute(Query2)
        db.commit()
        t1=access.fetchall()
        data=pd.DataFrame(t1,columns=["sum","channel "])
        st.write(data)
        
    elif Queries == "3) The top 10 most viewed videos and their respective channels": 
        Query3='''select channelid,channel_name,view_count from videos inner join channels on videos.channelid = channels.channel_id where view_count is not null  order by view_count  desc limit 10'''
        access.execute(Query3)
        db.commit()
        t1=access.fetchall()
        data=pd.DataFrame(t1,columns=["channelid","channelname ","viewcount"])
        st.write(data)
        
    elif Queries == "4) Comments were made on each video and their corresponding video names":
        Query4='''select comment_count,video_id,video_name from videos where comment_count is not null order by comment_count desc'''
        access.execute(Query4)
        db.commit()
        t1=access.fetchall()
        data=pd.DataFrame(t1,columns=["commentcount"," videoid","videoname"])
        st.write(data)
        
    elif Queries == "5) Videos have the highest number of likes and their corresponding channel names":
        Query5='''select like_count,channel_name from videos inner join channels on videos.channelid = channels.channel_id order by like_count desc'''
        access.execute(Query5)
        db.commit()
        t2=access.fetchall()
        data1=pd.DataFrame(t2,columns=["likecount","channelname"])
        st.write(data1)
        
    elif Queries == "6) The total number of likes and dislikes for each video and their corresponding video names":
        Query6='''select like_count,video_name from videos  where like_count is not null order by like_count desc limit 20'''
        access.execute(Query6)
        db.commit()
        t2=access.fetchall()
        data1=pd.DataFrame(t2,columns=["likecount","videoname"])
        st.write(data1)
        
    elif Queries == "7) The total number of views for each channel and their corresponding channel names":
        Query7='''select channel_views,channel_name from channels'''
        access.execute(Query7)
        db.commit()
        t2=access.fetchall()
        data1=pd.DataFrame(t2,columns=["channelviews","channelname"])
        st.write(data1)
        
    elif Queries == "8) The names of all the channels that have published videos in the year 2022":
        Query8='''select channel_name,publishedat from videos inner join channels on videos.channelid = channels.channel_id where EXTRACT(year from publishedat)= 2022'''
        access.execute(Query8)
        db.commit()
        t2=access.fetchall()
        data1=pd.DataFrame(t2,columns=["channelname","publishedat"])
        st.write(data1)
        
    elif Queries == "9) The average duration of all videos in each channel and their corresponding channel names":
        Query9='''select channel_name,avg(duration) from videos inner join channels on videos.channelid = channels.channel_id group by channel_name;'''
        access.execute(Query9)
        db.commit()
        t2=access.fetchall()
        data1=pd.DataFrame(t2,columns=["channelname","Avg"])
        st.write(data1)
        
    elif Queries ==  "10)Videos have the highest number of comments and their corresponding channel names":
        Query10='''select comment_count,channel_name,video_name from videos inner join channels on videos.channelid = channels.channel_id where comment_count is not null order by comment_count  desc limit 1'''
        access.execute(Query10)
        db.commit()
        t2=access.fetchall()
        data1=pd.DataFrame(t2,columns=["commentcount","channelname","videoname"])
        st.write(data1)