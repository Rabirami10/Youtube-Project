import streamlit as st
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
import YTdata
import pymongodb
import pandas as pd
import MySqlConnector


def main():
    st.title(':red[YouTube Data Harvesting]')
    channel_id = st.sidebar.text_input("Enter YouTube Channel ID")
    retrieve_data_button = st.sidebar.button("Get Data")
    move_to_mongodb_button = st.sidebar.button("Move to MongoDb")
    enable_ch_name = st.sidebar.checkbox("Enable channel Name")
    select_channel_name = st.sidebar.selectbox('channel name', pymongodb.fetch_ch_name())
    migrate_button = st.sidebar.button('Migrate to Sql')
    enable_select = st.sidebar.checkbox("Enable to Questions")

    q1 = "What are the names of all the videos and their corresponding channels?"
    q2 = "Which channels have the most number of videos, and how many videos do they have?"
    q3 = "What are the top 10 most viewed videos and their respective channels?"
    q4 = "How many comments were made on each video, and what are their corresponding video names?"
    q5 = "Which videos have the highest number of likes, and what are their corresponding channel names?"
    q6 = "What is the total number of likes for each video, and what are their corresponding video names?"
    q7 = "What is the total number of views for each channel, and what are their corresponding channel names?"
    q8 = "What are the names of all the channels that have published videos in the year 2022?"
    q9 = "What is the average duration of all videos in each channel, and what are their corresponding channel names?"
    q10 = "Which videos have the highest number of comments, and what are their corresponding channel names?"

    select_box = st.sidebar.selectbox("Select a Question", [q1, q2, q3, q4, q5, q6, q7, q8, q9, q10])
    if channel_id:
        channel_data = YTdata.get_channel_info(channel_id)
        video_ids = YTdata.get_video_id(channel_data)
        video_details = YTdata.get_video_details(video_ids)
        comment_details = YTdata.get_comment_details(video_ids)
        retrieved_data = YTdata.merge_data(channel_data, video_details, comment_details)

        if retrieve_data_button:
            st.write(retrieved_data)
        if move_to_mongodb_button:
            connection = pymongodb.create_mongodb_connection()
            # Insert the document into the collection
            pymongodb.push_to_mongodb(connection, retrieved_data)
            st.success('Moved to MongoDB!')

    if enable_ch_name:
        if select_channel_name:
            query = {"Channel_Details.channel_name": select_channel_name}
            doc = pymongodb.create_mongodb_connection().find_one(query)
            st.write(doc)
            if migrate_button:
                connection = MySqlConnector.mysql_connector()
                ch = doc["Channel_Details"]
                vd = doc["Video_Details"]
                cmt = doc["Comment_Details"]
                ch_df = pd.DataFrame([ch])
                vd_df = pd.DataFrame(vd)
                vd_df['duration'] = vd_df['duration'].apply(YTdata.format_duration)
                cmt_df = pd.DataFrame(cmt)
                try:
                    ch_df.to_sql(name='channel', con=connection, if_exists='append', index=False)
                    vd_df.to_sql(name='video', con=connection, index_label=None, if_exists='append', index=False)
                    cmt_df.to_sql(name='comment', con=connection, index_label=None, if_exists='append', index=False)
                    st.success("Data inserted successfully!")
                except IntegrityError:
                    # Handle the IntegrityError exception to avoid duplicate records
                    st.error("Error: Duplicate entry. The record already exists.")
    if enable_select:
        if select_box == q1:
            query = text(
                "SELECT v.video_Title, c.channel_name FROM video v JOIN channel c ON v.channel_id = c.channel_id;")
            result = MySqlConnector.execute_query(query)
            st.write(q1)
            st.dataframe(result)
        elif select_box == q2:
            query = text(
                "select channel_name, Total_videos from channel where Total_videos = (select MAX(Total_videos) from channel);")
            result = MySqlConnector.execute_query(query)
            st.write(q2)
            st.dataframe(result)
        elif select_box == q3:
            query = text(
                "SELECT v.video_Title, v.view_count ,c.channel_name FROM video v JOIN channel c ON v.channel_id = c.channel_id ORDER BY v.view_count DESC LIMIT 10;")
            result = MySqlConnector.execute_query(query)
            st.write(q3)
            st.dataframe(result)
        elif select_box == q4:
            query = text('SELECT video_Title, comment_count FROM video ')
            result = MySqlConnector.execute_query(query)
            st.write(q4)
            st.dataframe(result)
        elif select_box == q5:
            query = text(
                'SELECT v.video_Title, v.like_count, c.channel_name FROM video v JOIN channel c ON v.channel_id = c.channel_id WHERE v.like_count = (SELECT MAX(like_count) FROM video);')
            result = MySqlConnector.execute_query(query)
            st.write(q5)
            st.dataframe(result)
        elif select_box == q6:
            query = text('SELECT video_Title, like_count as Total_likes FROM video;')
            result = MySqlConnector.execute_query(query)
            st.write(q6)
            st.dataframe(result)
        elif select_box == q7:
            query = text('SELECT Channel_name, Total_views from channel;')
            result = MySqlConnector.execute_query(query)
            st.write(q7)
            st.dataframe(result)
        elif select_box == q8:
            query = text(
                'SELECT  c.channel_name FROM channel c JOIN video v ON c.channel_id = v.channel_id WHERE YEAR(v.published_At) = 2022;')
            result = MySqlConnector.execute_query(query)
            st.write(q8)
            st.dataframe(result)
        elif select_box == q9:
            query = text(
                'SELECT c.channel_name, AVG(v.duration) AS average_duration FROM channel c JOIN video v ON c.channel_id = v.channel_id GROUP BY c.channel_name;')
            result = MySqlConnector.execute_query(query)
            st.write(q9)
            st.dataframe(result)
        elif select_box == q10:
            query = text(
                'select v.video_Title, v.comment_count, c.channel_name FROM video v JOIN channel c ON v.channel_id = c.channel_id WHERE v.comment_count = (SELECT MAX(comment_count) FROM video);')
            result = MySqlConnector.execute_query(query)
            st.write(q10)
            st.dataframe(result)


if __name__ == "__main__":
    main()
