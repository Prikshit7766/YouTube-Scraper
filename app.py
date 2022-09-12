from flask import Flask, render_template, request,jsonify
from flask_cors import CORS,cross_origin
import  pytube
import os
import requests
from pytube import YouTube
from pytube import Channel
from pytube import extract
from apiclient.discovery import build
import pafy
import re
import pymongo
import base64
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import requests
import json


app = Flask(__name__)


@app.route('/',methods=['GET'])  # route to display the home page
@cross_origin() # cross orign is not required in case of local deployment but when you deploy these things into  cloud then these things will be required
# when ever you want to connect from one orign to another orign so this  cross_origin attribute is imp
def homePage():
    return render_template("index.html")


@app.route('/submit',methods=['POST','GET'])
def index():
    # read the posted values
    if request.method=='POST':
        channel_link=request.form['link']
        num=request.form['num']
        api_key = "AIzaSyBRavSeTJSC9exG0IZS1H8WJcJvAtiEx_I"  # Replace this dummy api key with your own.
        author_box = [['author', 'video_title', 'views', 'duration', 'likes', 'Total_Comment', 'video_number']]
        youtube = build('youtube', 'v3', developerKey=api_key)
        loc = r"./videos"
        target_folder = r"./images"

        def channel_info(channel_link):
            channel = Channel(channel_link)
            author = channel.channel_name

            ## channel name
            print(channel.channel_name)

            ## channel url
            channel_URL=channel.channel_url
            print(channel.channel_url)
            Total_videos = len(channel.video_urls)
            print(len(channel.video_urls))
            return channel, author ,Total_videos,channel_URL

        def persist_image(folder_path: str, url: str, counter):  # for storing the images
            try:
                image_content = requests.get(url).content

            except Exception as e:
                print(f"ERROR - Could not download {url} - {e}")

            try:
                f = open(os.path.join(folder_path, 'jpg' + "_" + str(counter) + ".jpg"), 'wb')
                f.write(image_content)
                f.close()
                print(f"SUCCESS - saved {url} - as {folder_path}")
            except Exception as e:
                print(f"ERROR - Could not save {url} - {e}")

        def image_to_bas64_to_mongodb(Title, thumbnail):
            b64_string = base64.b64encode(requests.get(thumbnail).content)
            data = {f'{Title}': f'{b64_string}'}
            client = pymongo.MongoClient(
                "mongodb+srv://Prikshit7766:prikshit@cluster0.bb7u7jb.mongodb.net/?retryWrites=true&w=majority")
            db = client.test
            database = client["challange"]
            collection = database["thumnail"]
            collection.insert_one(data)

        def title_(url, counter):
            yt = YouTube(url)
            Title = yt.title
            thumbnail = yt.thumbnail_url
            persist_image(target_folder, thumbnail, counter)
            image_to_bas64_to_mongodb(Title, thumbnail)
            return Title

        def scrape_comments_with_replies(ID, box, searchString, III):
            filename = re.sub(r"[-()\"#/@;:<>{}`+=~|.!?,]", "", searchString) + ".csv"
            print(filename)

            data = youtube.commentThreads().list(part='snippet', videoId=ID, maxResults='100',
                                                 textFormat="plainText").execute()

            for i in data["items"]:

                name = i["snippet"]['topLevelComment']["snippet"]["authorDisplayName"]
                comment = i["snippet"]['topLevelComment']["snippet"]["textDisplay"]
                published_at = i["snippet"]['topLevelComment']["snippet"]['publishedAt']
                likes = i["snippet"]['topLevelComment']["snippet"]['likeCount']
                replies = i["snippet"]['totalReplyCount']

                box.append([name, comment, published_at, likes, replies])

                totalReplyCount = i["snippet"]['totalReplyCount']

                if totalReplyCount > 0:

                    parent = i["snippet"]['topLevelComment']["id"]

                    data2 = youtube.comments().list(part='snippet', maxResults='100', parentId=parent,
                                                    textFormat="plainText").execute()

                    for i in data2["items"]:
                        name = i["snippet"]["authorDisplayName"]
                        comment = i["snippet"]["textDisplay"]
                        published_at = i["snippet"]['publishedAt']
                        likes = i["snippet"]['likeCount']
                        replies = ""

                        box.append([name, comment, published_at, likes, replies])

            while ("nextPageToken" in data):

                data = youtube.commentThreads().list(part='snippet', videoId=ID, pageToken=data["nextPageToken"],
                                                     maxResults='100', textFormat="plainText").execute()

                for i in data["items"]:
                    name = i["snippet"]['topLevelComment']["snippet"]["authorDisplayName"]
                    comment = i["snippet"]['topLevelComment']["snippet"]["textDisplay"]
                    published_at = i["snippet"]['topLevelComment']["snippet"]['publishedAt']
                    likes = i["snippet"]['topLevelComment']["snippet"]['likeCount']
                    replies = i["snippet"]['totalReplyCount']

                    box.append([name, comment, published_at, likes, replies])

                    totalReplyCount = i["snippet"]['totalReplyCount']

                    if totalReplyCount > 0:

                        parent = i["snippet"]['topLevelComment']["id"]

                        data2 = youtube.comments().list(part='snippet', maxResults='100', parentId=parent,
                                                        textFormat="plainText").execute()

                        for i in data2["items"]:
                            name = i["snippet"]["authorDisplayName"]
                            comment = i["snippet"]["textDisplay"]
                            published_at = i["snippet"]['publishedAt']
                            likes = i["snippet"]['likeCount']
                            replies = ''

                            box.append([name, comment, published_at, likes, replies])

            df = pd.DataFrame(
                {'Name': [i[0] for i in box], 'Comment': [i[1] for i in box], 'Timing': [i[2] for i in box],
                 'Likes': [i[3] for i in box], 'ReplyCount': [i[4] for i in box]})
            df.to_csv(f'{filename}', index=False, header=False)
            save_video_dataframe(df, III)

            return "Successful! Check the CSV file that you have just created."

        def save_video_dataframe(df, ID):
            print('opening...')

            print('open snowflake')
            cnn = snowflake.connector.connect(
                user='PRIKSHIT7766',
                password='Prikshit7766',
                account='gjkfzhe-bd01074',
                database='challange',
                role='ACCOUNTADMIN',
                Warehouse='MY_WH',
                schema='myschema',

            )
            df = df.astype(str)
            tablename = 'video' + str(ID)
            print(tablename, type(tablename))
            tablename = tablename.upper()
            cur = cnn.cursor()

            cur.execute(
                f"create table if not exists {tablename} (Name varchar(100),Comment varchar(22000),Timing varchar(100),Likes varchar(50),ReplyCount varchar(50))")
            cur.execute('use warehouse MY_WH')
            success, nchunks, nrows, _ = write_pandas(cnn, df, tablename, quote_identifiers=False)
            print(str(success) + ',' + str(nchunks) + ',' + str(nrows))
            cnn.close()
            print('done')

        def comm_fun(url, Title, i):

            ID = extract.video_id(url)  # Replace this YouTube video ID with your own.
            print(ID)

            box = [
                ['Name', 'Comment', 'Timing', 'Likes', 'ReplyCount']]  # where old comments and replies will be stored

            scrape_comments_with_replies(ID, box, Title, i)

        def video(yt, loc):
            yt.streams.get_highest_resolution().download(loc)
            print("video downloaded")

        def author_csv(url, T, i):
            video = YouTube(url)
            video_ = pafy.new(url)
            Title = video.title

            Views = video.views
            Author = video.author
            Duration = video_.duration
            Likes = video_.likes
            df = pd.read_csv(re.sub(r"[-()\"#/@;:<>{}`+=~|.!?,]", "", T) + ".csv")
            Total_Comment = len(df)
            video_number = i

            return Author, Title, Views, Duration, Likes, Total_Comment, video_number

        def call(z):
            counter = 0
            try:
                channel, author ,Total_videos ,channel_URL= channel_info(channel_link)

                for url in channel.video_urls[:z]:

                    print(url)
                    try:
                        yt = pytube.YouTube(url)
                    except pytube.exceptions.VideoPrivate:
                        print('video is Private')
                    except pytube.exceptions.VideoRegionBlocked:
                        print('Video is blocked')
                    except pytube.exceptions.VideoUnavailable:
                        print("video Unavailable")
                    else:
                        Title = title_(url, counter)
                        comm_fun(url, Title, counter)
                        video(yt, loc)
                        Author, Title, Views, Duration, Likes, Total_Comment, video_number = author_csv(url, Title,
                                                                                                        counter)
                        author_box.append([Author, Title, Views, Duration, Likes, Total_Comment, video_number])

                        counter += 1
                return author_box, Author,Total_videos,channel_URL
            except Exception as e:
                print(e)

        def upload_video_gdrive():

            i = 0
            for dirname, _, filenames in os.walk(r"./videos"):

                for filename in filenames:
                    full_path = os.path.join(dirname, filename)
                    print(full_path)

                    headers = {
                        "Authorization": "Bearer ya29.a0AVA9y1tspQccgkt8HCea0S3q8TFxaAwx3RNUFra7eEKibhdX6a0hCr7cDlZLLS3OSHvYBx6_sE8QOqz3cxwdCKP1PBpUpbi7KImgq1HF9G6DOLoxlQwwUFgPNlhCsciUZ3yuzDQSpnWPRCF6wE2mGAnjG4_KaCgYKATASARASFQE65dr8-YCXY9JA-DRW9BAL28Vz3g0163"}  # put ur access token after the word 'Bearer '
                    para = {
                        "name": filename,  # file name to be uploaded
                        "parents": ["1A_5Lqy-AVNkKh1qrhmCQljRZ3-8vXMwK"]
                        # make a folder on drive in which you want to upload files; then open that folder; the last thing in present url will be folder id
                    }

                    files = {
                        'data': ('metadata', json.dumps(para), 'application/json; charset=UTF-8'),
                        'file': ('application/zip', open(full_path, "rb"))
                        # replace 'application/zip' by 'image/png' for png images; similarly 'image/jpeg' (also replace your file name)
                    }
                    r = requests.post(
                        "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart",
                        headers=headers,
                        files=files
                    )
                    print(r.text)
                    i = i + 1
            print(i)

        def upload_auther_csv(df, Author):
            print('opening...')

            print('open snowflake')
            cnn = snowflake.connector.connect(
                user='PRIKSHIT7766',
                password='Prikshit7766',
                account='gjkfzhe-bd01074',
                database='challange',
                role='ACCOUNTADMIN',
                Warehouse='MY_WH',
                schema='myschema',

            )
            df = df.astype(str)
            tablename = Author
            print(tablename, type(tablename))
            tablename = tablename.upper()
            cur = cnn.cursor()

            cur.execute(
                f"create table if not exists {tablename} (author varchar(100),video_title varchar(500),views varchar(100),duration  varchar(100),likes  varchar(100),Total_Comment varchar(200),video_number varchar(200))")
            cur.execute('use warehouse MY_WH')
            success, nchunks, nrows, _ = write_pandas(cnn, df, tablename, quote_identifiers=False)
            print(str(success) + ',' + str(nchunks) + ',' + str(nrows))
            cnn.close()
            print('done')

        try:
            channel_box, Author,Total_videos ,channel__URL= call(int(num))
            filename = Author + ".csv"
            data = pd.DataFrame({'author': [i[0] for i in channel_box], 'video_title': [i[1] for i in channel_box],
                                 'views': [i[2] for i in channel_box], 'duration': [i[3] for i in channel_box],
                                 'likes': [i[4] for i in channel_box],
                                 'Total_Comment': [i[5] for i in channel_box],
                                 'video_number': [i[6] for i in channel_box]})

            upload_auther_csv(data, Author)
            data.to_csv(f'{filename}', index=False, header=False)
            upload_video_gdrive()
        except Exception as e:
            print(e)
            return render_template("for error.html",tt=e)

        return  render_template("result.html",channel_name=Author,Total_video=Total_videos,channel_URL=channel__URL,tables=[data.to_html(classes='data',index=False, header=False)])


    else:
        return render_template('index.html')


if __name__ == "__main__":
    #app.run(host='127.0.0.1', port=8001, debug=True)
	app.run(host='127.0.0.1', port=8001, debug=True)