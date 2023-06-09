import pymongo
from pprint import pprint

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client['as']
movies = db['movies']


# Print the title and IMDB rating for each movie of top N movies
def Find_top_N_movies_with_the_highest_IMDB_rating(n):
    pipe = [
        {"$match": {"imdb.rating": {"$ne": ""}}},
        {"$sort": {"imdb.rating": -1}},
        {"$limit": n },
        {"$project" : {"id" : "$_id","title" : "$title" , "imdb-rating" : "$imdb.rating"}}

    ]
    pprint(list(movies.aggregate(pipe)))

print('''Printing the title and IMDB rating for each movie of top N movies
''')
print()
Find_top_N_movies_with_the_highest_IMDB_rating(10)

print()
print()
print("-------------------------------------------------------------------------------------------------------------------")

from pprint import pprint

def find_top_n_movies_with_highest_imdb_rating_in_year(n,year):
    pipe = [
        {"$match": {"$and" : [{"imdb.rating": {"$ne": ""}},{"year" : 2002 }]}},
        {"$sort": {"imdb.rating": -1}},
        {"$limit": n },
        {"$project": {"id": "$_id", "title": "$title", "imdb-rating": "$imdb.rating","year" : "$year" }}

    ]
    pprint(list(movies.aggregate(pipe)))

print('''Printing the title and IMDB rating for each movie of top N movies in year 2002
''')
print()
find_top_n_movies_with_highest_imdb_rating_in_year(10,2002)

print()
print()
print("-------------------------------------------------------------------------------------------------------------------")

def find_top_n_movies_with_highest_imdb_rating_in_votes_greater_than_1000(n):
    pipe = [
        {"$match": {"$and" : [{"imdb.rating": {"$ne": ""}},{"imdb.votes" :{"$gte" : 1000 }}  ]}},
        {"$sort": {"imdb.rating": -1}},
        {"$limit": n },
        {"$project": {"id": "$_id", "title": "$title", "imdb-rating": "$imdb.rating","imdb-votes": "$imdb.votes" }}

    ]
    pprint(list(movies.aggregate(pipe)))
print('''Printing the title and IMDB rating with votes greater than 1000
''')
print()
find_top_n_movies_with_highest_imdb_rating_in_votes_greater_than_1000(3)


print()
print()
print("-------------------------------------------------------------------------------------------------------------------")

def find_top_movies_by_title_pattern(pattern, limit=10):
    pipeline = [
        {"$match": {"title": {"$regex": pattern}}},
        {"$sort": {"tomatoes.viewer.rating": -1}},
        {"$limit": limit},
        {"$project": {"_id": 0,"title": 1,"imdb.rating": 1, "viewer_rating" : "$tomatoes.viewer.rating"}}
    ]
    results = db.movies.aggregate(pipeline)
    for doc in results:
        print(doc)
print('''
find_top_movies_by_title_pattern using regex''')
print()
find_top_movies_by_title_pattern("The")

print()
print()
print("-------------------------------------------------------------------------------------------------------------------")

def find_top_N_directors():
    pipeline2 = [

        {"$match": {"directors": {"$ne": None}}},
        {
            "$group": {
                "_id": "$directors",
                "count": {"$sum": 1},
            }
        },
        {
            "$sort": {
                "count": pymongo.DESCENDING
            }
        },
        {
            "$limit": 10
        },


    ]

    pprint(list(movies.aggregate(pipeline2)))

print('''
find_top_N_directors with most number of movies''')
print()
find_top_N_directors()


print()
print()
print("-------------------------------------------------------------------------------------------------------------------")

def find_top_N_directors_in_year(year):
    pipeline2 = [

        {"$match": {"year" : year}},
        {"$unwind": "$directors"},
        {
            "$group": {
                "_id": "$directors",
                "count": {"$sum": 1},
            }
        },
        {
            "$sort": {
                "count": pymongo.DESCENDING
            }
        },
        {
            "$limit": 4
        },
        {"$project": {"count": "$count","year" : "$year"}}

    ]

    pprint(list(movies.aggregate(pipeline2)))

print('''
find_top_N_directors_in_year that is year of 2002
''')
print()
find_top_N_directors_in_year(2002)


print()
print()
print("-------------------------------------------------------------------------------------------------------------------")

def find_top_N_directors_in_genre(genre):
    pipeline2 = [
        {"$unwind": "$genres"},
        {"$match": {"genres" : genre}},
        {
            "$group": {
                "_id": "$directors",
                "count": {"$sum": 1},
            }
        },
        {
            "$sort": {
                "count": pymongo.DESCENDING
            }
        },
        {
            "$limit": 4
        },
        {"$project": {"count": "$count"}}

    ]

    pprint(list(movies.aggregate(pipeline2)))

print('''
find_top_N_directors_in_year that is genre that is crime
''')
print()
find_top_N_directors_in_genre("Crime")

print()
print()
print("-------------------------------------------------------------------------------------------------------------------")

def top_n_actors_with_max_movies(n):
    pipe = [
        {"$unwind": "$cast"},
        {"$group" : {"_id" : "$cast","count" : {"$sum" : 1}}},
        {"$sort": {"count": -1}},
        {"$limit": n },
    ]
    pprint(list(movies.aggregate(pipe)))


print(''''
top_n_actors_with_max_movies in this :''')
print()
top_n_actors_with_max_movies(10)

print()
print()
print("-------------------------------------------------------------------------------------------------------------------")

def top_n_actors_with_max_movies_in_year(n,year):
    pipe = [
        {"$match": {"year": year}},
        {"$unwind": "$cast"},
        {"$group" : {"_id" : "$cast","count" : {"$sum" : 1}}},
        {"$sort": {"count": -1}},
        {"$limit": n },
    ]
    pprint(list(movies.aggregate(pipe)))

print(''''
top_n_actors_with_max_movies in this : 2002''')
print()
top_n_actors_with_max_movies_in_year(10,2002)


print()
print()
print("-------------------------------------------------------------------------------------------------------------------")

def top_n_actors_with_max_movies_in_year_genre(n,genre):
    pipe = [
        {"$unwind": "$genres"},
        {"$match": {"genres": genre}},
        {"$unwind": "$cast"},
        {"$group" : {"_id" : "$cast","count" : {"$sum" : 1}}},
        {"$sort": {"count": -1}},
        {"$limit": n },
        {"$project" : {"genre" : "$genres","count" : "$count"}}
    ]
    pprint(list(movies.aggregate(pipe)))

print(''''
Top_n_actors_with_max_movies in the genre :''')
print()
top_n_actors_with_max_movies_in_year_genre(10,"Crime")


print()
print("-------------------------------------------------------------------------------------------------------------------")

def topNMoviesForAGenre(N):
        pipe=[
            {"$unwind":"$genres"},
            {"$group":{"_id":"$genres"}}
        ]
        for i in list(movies.aggregate(pipe)):
            genre=i['_id']
            print("Genre: "+genre)
            pipe=[
                {"$match":{"genres":genre}},
                {"$sort":{"imdb.rating":-1}},
                {"$match":{"imdb.rating":{"$ne":""}}},
                {"$project":{"_id":0,"title":1,"rating":"$imdb.rating"}},
                {"$limit":N}
            ] 
            pprint(list(movies.aggregate(pipe)))
print(''''
topNMoviesForAGenre : ''')
topNMoviesForAGenre(10)











