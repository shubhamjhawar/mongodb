import pymongo
from pprint import pprint

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client['as']
theaters = db['theatres']

# def Find_10_cities_top_theaters():
#     pipe = [
#         {"$group": {"_id" : "$location.address.city" , "cnt":{"$sum":1}}},
#         {"$sort": {"cnt": -1}},
#         {"$limit": 10 },
#     ]
#     pprint(list(theaters.aggregate(pipe)))
#
# Find_10_cities_top_theaters()


def top10theatersNear(cod):
    theaters.create_index([("location.geo", "2dsphere")])
    pprint(list(theaters.find(
        {
            "location.geo": {
                "$near": {
                    "$geometry": {
                        "type": "Point",
                        "coordinates": cod
                    }}
            }
        }).limit(10)))

top10theatersNear([-111.89966,33.430729])
