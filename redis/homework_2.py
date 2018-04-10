import redis
import datetime


ONE_WEEK_IN_SECONDS = 7 * 86400
VOTE_SCORE = 432

def article_vote(redis, user, article):
    cutoff = datetime.datetime.now() - datetime.timedelta(seconds=ONE_WEEK_IN_SECONDS)

    if not datetime.datetime.fromtimestamp(redis.zscore('time:', article)) < cutoff:
        article_id = article.split(':')[-1]
        if redis.sadd('voted:' + article_id, user):
            redis.zincrby(name='score:', value=article, amount=VOTE_SCORE)
            redis.hincrby(name=article, key='votes', amount=1)

def article_switch_vote(redis, user, from_article, to_article):
    # HOMEWORK 2 Part I
    cutoff = datetime.datetime.now() - datetime.timedelta(seconds=ONE_WEEK_IN_SECONDS)

    if not datetime.datetime.fromtimestamp(redis.zscore('time:', to_article)) < cutoff:
        from_article_id = from_article.split(':')[-1]
        #to_article_id = to_article.split(':')[-1]
        if redis.sismember('voted:' + from_article_id, user):
            redis.zincrby(name='score:', value=from_article, amount = -(VOTE_SCORE))
            redis.hincrby(name=from_article, key='votes', amount = -1)
            redis.srem('voted:' + from_article_id, user)
        article_vote(redis, user, to_article)
    pass

ARTICLES_PER_PAGE = 25

def get_articles(redis, page, order='score:'):
    start = (page-1) * ARTICLES_PER_PAGE
    end = start + ARTICLES_PER_PAGE - 1

    ids = redis.zrevrange(order, start, end)
    articles = []
    for id in ids:
        article_data = redis.hgetall(id)
        article_data['id'] = id
        articles.append(article_data)
    return articles


redis = redis.StrictRedis(host='localhost', port=6379, db=0)

#print(get_articles(redis,1,order='score:'))

# user:3 up votes article:1
article_vote(redis, "user:3", "article:1")
# user:3 up votes article:3
article_vote(redis, "user:3", "article:3")
# user:2 switches their vote from article:8 to article:1
article_switch_vote(redis, "user:2", "article:8", "article:1")

#print("user:2 has voted on article:8, y/n: " + str(redis.sismember('voted:8', 'user:2')))
#print(get_articles(redis,1,order='score:'))

# Which article's score is between 10 and 20?
# PRINT THE ARTICLE'S LINK TO STDOUT:
# HOMEWORK 2 Part II

print(str(redis.zrangebyscore('score:',10,20,withscores=True)))

# article = redis.?
# print redis.?

