package thepieuvre.nlp

import thepieuvre.nlp.util.UniqueMath

import redis.clients.jedis.Jedis
import redis.clients.jedis.Pipeline
import redis.clients.jedis.Response

class RedisHelper {

	RedisHelper() {
	}

	private def fetchingGram(long article, String type) {
		Jedis redis = NLProcessor.pool.getResource()
		try {
			def res = []
			String key = "article:$article:$type"
			int end = redis.zcard(key)
			def range = redis.zrevrangeWithScores(key, 0, end)

			Pipeline p = redis.pipelined()
			range.each {
				def elem = [:]
				elem.name = it.getElement()
				elem.score = it.getScore()
				elem.articles = p.lrange("chunk:$type:$elem.name", 0, -1)
				res << elem
			}
			p.sync()
			res.each {
				def articles = []
				it.articles.get().each { resp ->
					String art = resp
					if (art != "article:$article") {
						def artId = art.split(':')[1] 
						if (artId) {
							articles << (artId as long)
						}
					}
				}
				//it.articles = articles.unique()
				it.articles = UniqueMath.unique(articles) { v ->
					v
				}
			}
			return res
		} catch (Exception e) {
			e.printStackTrace()
		} finally {
			NLProcessor.pool.returnResource(redis)
		}
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