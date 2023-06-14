import pymongo


def create_mongodb_connection():  # Creating connection to mongodb
    client = pymongo.MongoClient('localhost', 27017)
    db = client["youtube_data"]
    mycol = db["channel_details"]
    return mycol


def push_to_mongodb(mongodb, final_data):  # Pushing the retrived data to mongodb
    store_in_mongodb = mongodb.insert_one(final_data)
    return store_in_mongodb


def fetch_ch_name():  # To fetch channel name in all doc from collection
    get_channel_name = create_mongodb_connection().find({}, {"Channel_Details.channel_name": 1})
    channel_list = []
    for i in get_channel_name:
        channel_name = i["Channel_Details"]["channel_name"]
        channel_list.append(channel_name)
    return channel_list
