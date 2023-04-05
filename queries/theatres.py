import pymongo
from pprint import pprint

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client['as']
theaters = db['theatres']

def Find_10_cities_top_theaters():
    pipe = [
        {"$group": {"_id" : "$location.address.city" , "cnt":{"$sum":1}}},
        {"$sort": {"cnt": -1}},
        {"$limit": 10 },
        {"$project" : {"city" : "$location.address.city","count" : "$cnt"}},
    ]
    pprint(list(theaters.aggregate(pipe)))
print('''
Find_10_cities_top_theaters in this
''' )

Find_10_cities_top_theaters()

print()
print()
print("-------------------------------------------------------------------------------------------------------------------")


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
    },{"theaterId" : 1 ,"location.geo.coordinates" : 1}).limit(10)))

print('''
top10theatersNear the location  [-111.89966,33.430729]
''')
top10theatersNear([-111.89966,33.430729])



