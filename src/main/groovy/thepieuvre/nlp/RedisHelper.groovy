package thepieuvre.nlp

import redis.clients.jedis.Jedis

class RedisHelper {

	Jedis redis

	RedisHelper() {
		redis = new Jedis("localhost")
	}

	private def fetchingGram(long article, String type) {
		def res = []
		String key = "article:$article:$type"
		int end = redis.zcard(key)
		redis.zrevrangeWithScores(key, 0, end). each {
			def elem = [:]
			elem.name = it.getElement()
			elem.score = it.getScore()
			elem.articles = []
			String chunk = ""
			long last = redis.llen("chunk:$type:$elem.name")
			redis.lrange("chunk:$type:$elem.name", 0, last) .each { art ->
				if (art != "article:$article") {
					def artId = art.split(':')[1] 
					if (artId) {
						elem.articles << (artId as long)
					}
				}
			}
			elem.articles.unique()
			res << elem
		}
		return res
	}

	def getUniGram(long article) {
		fetchingGram(article, 'unigram')
	}

	def getBiGram(long article) {
		fetchingGram(article, 'bigram')
	}

	def getNGram(long article) {
		fetchingGram(article, 'ngram')
	}

	def getTrainedGram(long article) {
		fetchingGram(article, 'trainedgram')
	}
}