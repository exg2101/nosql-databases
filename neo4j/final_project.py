# Put the use case you chose here. Then justify your database choice:
# I chose the Instgram use case and used neo4j because, as a social network, a photosharing app would easily and
# logically by modeled using a graph database. Interactions on the app, such as follows, likes, and comments,
# are easily modeled as relationships while users and posts can be modeled as nodes, as those are the objects
# that users interact with. (I  did some research and found that Instagram actually uses Cassandra as
# their primary database, but I still chose neo4j because of the way it logically maps onto a social network,
# at least on a small scale. I recognize that something as massive as Instagram would likely require a
# different kind of database that can handle the huge volume of users.)
#
# Explain what will happen if coffee is spilled on one of the servers in your cluster, causing it to go down.
# neo4j clusters have redundancy so that if one of the servers goes down, the database will continue to be 
# available. The overall speed of processing may be reduced, since there are fewer servers among which to
# distribute requests.
#
# What data is it not ok to lose in your app? What can you do in your commands to mitigate the risk of lost data?
# Of course, we never want to lose any data at all, but most crucial to this app is user information and
# the relationships between users and their posted content. We never want to have images floating around
# our server that are unconnected to any user. Actions such as likes, votes and comments, while important
# to the user experience, are less vital to the functioning of this app. One of the good things about neo4j 
# is that all Cypher queries that update the graph run in transactions. Either the whole query succeeds,
# or none of it does. This minimizes risk in the commands!
#
from neo4j.v1 import GraphDatabase
from datetime import datetime
import warnings
warnings.filterwarnings("ignore")


class Instagram(object):

	def __init__(self, uri, user, password):
		self._driver = GraphDatabase.driver(uri, auth=(user, password))

	def close(self):
		self._driver.close()

	def print_greeting(self, message):
		with self._driver.session() as session:
			greeting = session.write_transaction(self._create_and_return_greeting, message)
			print(greeting)

	@staticmethod
	def _create_and_return_greeting(tx, message):
		result = tx.run("CREATE (a:Greeting) "
						"SET a.message = $message "
						"RETURN a.message + ', from node ' + id(a)", message=message)
		return result.single()[0]

	def create_user(self, username, name, description):
		with self._driver.session() as session:
				user = session.write_transaction(self._create_user_node, username, name, description)
				print(user)

	@staticmethod
	def _create_user_node(tx, username, name, description):
		result = tx.run("CREATE (n:User) "
						"SET n.username = $username "
						"SET n.name = $name "
						"SET n.description = $description "
						"RETURN 'created user ' + n.username ", username=username, name=name, description=description)
		return result.single()[0]


	def post_photo(self, poster, image):
		with self._driver.session() as session:
				now = str(datetime.now())
				photo = session.write_transaction(self._create_photo_node, poster, image, now)
				print(photo)

	@staticmethod
	def _create_photo_node(tx, poster, image, now):
		result = tx.run("CREATE (b:Photo { image: $image, likes: 0, timestamp: $now } ) "
						"WITH b "
						"MATCH (a:User) "
						"WHERE a.username = $poster "
						"CREATE (a)-[r:posted]->(b) "
						"RETURN a.username + ' posted photo ' + b.image + ' at ' + $now", poster = poster, image = image, now = now)
		return result.single()[0]


	def post_story(self, poster, image, polltxt, op1, op2):
		with self._driver.session() as session:
				now = str(datetime.now())
				story = session.write_transaction(self._create_story_node, poster, image, polltxt, op1, op2, now)
				print(story)

	@staticmethod
	def _create_story_node(tx, poster, image, polltxt, op1, op2, now):
		if polltxt is '':
			result = tx.run("CREATE (b:Story { image: $image, timestamp: $now } ) "
							"WITH b "
							"MATCH (a:User) "
							"WHERE a.username = $poster "
							"CREATE (a)-[r:posted]->(b) "
							"RETURN a.username + ' posted story ' + b.image + ' at ' + $now", poster = poster, image = image, polltxt = polltxt, op1 = op1, op2 = op2, now = now)
		else:
			result = tx.run("CREATE (b:Story { image: $image, polltxt: $polltxt, timestamp: $now } ) "
							"WITH b "
							"MATCH (a:User) "
							"WHERE a.username = $poster "
							"CREATE (a)-[r:posted]->(b) "
							"CREATE (b)-[s:contains_poll]->(c:Poll { text: $polltxt, option1: $op1, option2: $op2, votes1: 0, votes2: 0 } ) "
							"RETURN a.username + ' posted story ' + b.image + ' at ' + $now + ' with poll: ' + $polltxt" , poster = poster, image = image, polltxt = polltxt, op1 = op1, op2 = op2, now = now)
		return result.single()[0]


	def like_photo(self, liker, image):
		with self._driver.session() as session:
			now = str(datetime.now())
			like = session.write_transaction(self._like_photo, liker, image, now)
			print(like)

	@staticmethod
	def _like_photo(tx, liker, image, now):
		result = tx.run("MATCH (a:User), (b:Photo) "
						"WHERE a.username = $liker AND b.image = $image "
						"CREATE (a)-[r:like { timestamp: $now } ]->(b) "
						"SET b.likes = b.likes + 1 "
						"RETURN a.username + ' liked ' + b.image ", liker = liker, image = image, now = now)
		return result.single()[0]

	def comment(self, user, image, message):
		with self._driver.session() as session:
			now = str(datetime.now())
			comment = session.write_transaction(self._comment, user, image, message, now)
			print(comment)

	@staticmethod
	def _comment(tx, user, image, message, now):
		result = tx.run("MATCH (a:User), (b:Photo) "
						"WHERE a.username = $user AND b.image = $image "
						"CREATE (a)-[r:comment { timestamp: $now, message: $message } ]->(b) "
						"RETURN a.username + ' commented <<' + $message + '>> on ' + b.image", user = user, image = image, message = message, now = now)
		return result.single()[0]

	def follow(self, user1, user2):
		with self._driver.session() as session:
			follow = session.write_transaction(self._follow, user1, user2)
			print(follow)

	@staticmethod
	def _follow(tx, user1, user2):
		result = tx.run("MATCH (a:User), (b:User) "
						"WHERE a.username = $user1 AND b.username = $user2 "
						"CREATE (a)-[r:follows]->(b) "
						"RETURN a.username + ' followed ' + b.username ", user1 = user1, user2 = user2)
		return result.single()[0]

	def vote_on(self, user, story, option):
		with self._driver.session() as session:
			vote = session.write_transaction(self._vote, user, story, option)
			print(vote)

	@staticmethod
	def _vote(tx, user, story, option):
		if option is 1:
			result = tx.run("MATCH (a:Story)-[r:contains_poll]->(b:Poll), (c:User) "
							"WHERE a.image = $story AND c.username = $user "
							"SET b.vote1 = b.vote1 + 1 "
							"CREATE (c)-[s:voted_on { option: $option }]->(b) "
							"RETURN c.username + ' voted for option ' + $option ", user = user, story = story, option = option)
			return result.single()[0]

		if option is 2:
			result = tx.run("MATCH (a:Story)-[r:contains_poll]->(b:Poll), (c:User) "
							"WHERE a.image = $story AND c.username = $user "
							"CREATE (c)-[s:voted_on { option: $option }]->(b) "
							"SET b.vote2 = b.vote2 + 1 "
							"RETURN c.username + ' voted for option ' + $option ", user = user, story = story, option = option)
			return result.single()[0]

	def view_profile(self, user):
		with self._driver.session() as session:
			profile = session.write_transaction(self._view_profile, user)
			print(str((list(profile))))

	@staticmethod
	def _view_profile(tx, user):
		result = tx.run("MATCH (a:User)-[r:posted]->(b:Photo) "
						"WHERE a.username = $user "
						"RETURN b.image", user = user)
		return result.records()

	def unfollow(self, user1, user2):
		with self._driver.session() as session:
			unfollow = session.write_transaction(self._unfollow, user1, user2)
			print(unfollow)

	@staticmethod
	def _unfollow(tx, user1, user2):
		result = tx.run("MATCH (a)-[r:follows]->(b) "
						"WHERE a.username = $user1 AND b.username = $user2 "
						"DELETE r "
						"RETURN a.username + ' unfollowed ' + b.username ", user1 = user1, user2 = user2)
		return result.single()[0]


testdb = Instagram("bolt://localhost:7687","neo4j","test")

testdb.create_user("alic3","Alice","columbia soccer 2018")
testdb.create_user("bobberific","Bob","machine learning and food eating")
testdb.create_user("carma","Carmen","yoga and dogs")

testdb.post_photo("alic3", "image1.jpg")
testdb.post_photo("alic3", "image2.jpg")
testdb.post_photo("bobberific", "image3.jpg")

testdb.post_story("carma", "story1.jpg", "which dog is cuter?", "left", "right")

testdb.like_photo("bobberific","image1.jpg")
testdb.like_photo("bobberific","image2.jpg")
testdb.like_photo("alic3","image3.jpg")

testdb.comment("bobberific","image2.jpg","love your shirt")

testdb.follow("bobberific","alic3")
testdb.follow("bobberific","carma")
testdb.follow("carma","bobberific")
testdb.follow("alic3","carma")

testdb.vote_on("alic3","story1.jpg",1)
testdb.vote_on("bobberific","story1.jpg",2)


# Action 1: A user, Debbie, signs up for a new account
testdb.create_user("debster","Debbie","clueless first-year")

# Action 2: Debbie follows some of her friends
testdb.follow("debster","alic3")
testdb.follow("debster","bobberific")
testdb.follow("debster","carma")

# Action 3: Debbie votes on the poll in Carmen's story
testdb.vote_on("debster","story1.jpg",2)

# Action 4: Debbie views Carmen's profile
testdb.view_profile("bobberific")

# Action 5: Debbie unfollows Carmen because she finds her posts boring
testdb.unfollow("debster","carma")

# Action 6: Debbie comments on one of Alice's images
testdb.comment("debster","image1.jpg","wow what a cool photo!")

# Action 7: Debbie posts her first photo
testdb.post_photo("debster","image20.jpg")

# Action 8: Bob posts a story
testdb.post_story("bobberific", "story2.jpg", "who else is excited Debbie is finally on IG?!", "me too", "me three")



