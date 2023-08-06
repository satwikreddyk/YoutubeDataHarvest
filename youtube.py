import streamlit as st
import pymongo
import sqlalchemy
import pandas as pd
import json

mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["Youtube"]

sql_engine = sqlalchemy.create_engine('mysql+pymysql://root:password@127.0.0.1:3306/youtube_data')

def get_youtube_data(channel_id, collection_name):
    data = {
        "Channel_Name": {
            "Channel_Id": channel_id,
            "Subscription_Count": 10000,
            "Channel_Views": 1000000,
           
        },
      
    }
    data_json = json.dumps(data)
    
    mongo_db[collection_name].insert_one({"data_json": data_json})
    
    return data_json

def migrate_to_mysql(channel_name, data_json):
    data = json.loads(data_json)
    
    df = pd.DataFrame(data)
    df.to_sql(channel_name, sql_engine, if_exists='replace', index=False)

def main():
    st.title("YouTube Data Analysis App")
    
    channel_id = st.text_input("Enter YouTube Channel ID:")
    collection_name = "channel" 
    if st.button("Get Data"):
        youtube_data_json = get_youtube_data(channel_id, collection_name)
        st.success("Data retrieved and stored in MongoDB.")
    
    channel_list = mongo_db.list_collection_names()
    selected_channel = st.selectbox("Select a Channel to Migrate to MySQL:", channel_list)
    
    if st.button("Migrate to MySQL"):
        data_to_migrate = mongo_db[selected_channel].find_one()["data_json"]
        
        migrate_to_mysql(selected_channel, data_to_migrate)
        
        st.success("Data migrated to MySQL database.")
    
    if st.button("Show Data from MySQL"):
        query = "SELECT * FROM " + selected_channel
        result_df = pd.read_sql_query(query, sql_engine)
        
        st.dataframe(result_df)

if __name__ == "__main__":
    main()
