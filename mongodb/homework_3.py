from pymongo import MongoClient

client = MongoClient()
db = client.test
collection = db.movies

#update "rated": "NOT RATED" to "Pending rating"
collection.update_many(
	{"rated": "NOT RATED"},
	{"$set": {"rated": "Pending rating"}})

#insert comedy movie
collection.insert_one(
	{"title": "Love, Simon",
	"year": "2018",
	"countries": ["USA"],
	"genres": ["Comedy","Drama","Romance"],
	"directors": ["Greg Berlanti"],
	"imdb": {"id": 5164432,
		"rating": 8.1,
		"votes": 13486}})

#find total number of comedy movies
print(list(collection.aggregate( [
	{"$match": { "genres": "Comedy" }},
	{"$group": {"_id":"Comedy", "count": { "$sum": 1 }}}
] ))[0])

#find number of movies made in the USA with a pending rating
print(list(collection.aggregate( [
	{"$match": {"countries": "USA", "rated": "Pending rating"}},
	{"$group": {"_id":{"country": "USA", "rating": "Pending rating"}, "count": {"$sum": 1}}}
] ))[0])

#$lookup example
students = db.students
grades = db.grades

students.insert_one({"name":"Alice","age":22,"year":"senior"})
students.insert_one({"name":"Beatrice","age":22,"year":"senior"})
students.insert_one({"name":"Carmine","age":20,"year":"sophomore"})
grades.insert_one({"student":"Alice","Calculus":"A","Economics":"B+"})
grades.insert_one({"student":"Beatrice","Calculus":"B","Economics":"A"})
grades.insert_one({"student":"Carmine","Calculus":"B-","Economics":"B"})

print(list(students.aggregate( [
	{"$lookup": 
		{"from":"grades",
		"localField": "name",
		"foreignField": "student",
		"as": "transcript"}
	}
])))
