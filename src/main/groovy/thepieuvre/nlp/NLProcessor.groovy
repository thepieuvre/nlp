package thepieuvre.nlp

import redis.clients.jedis.Jedis

import groovy.json.JsonBuilder

class NLProcessor {

	def redisMode() {
		println 'Starting listenning to the queue:extractor'
		Jedis redis = new Jedis("localhost")
		redis.sadd('queues', 'queue:nlp')
		while (true) {
			def task 
			try {
				task = redis.blpop(31415, 'queue:nlp')
				if (task) {
					println "${new Date()}.>>>>> ${task[1]}"
					AnalyzedArticle article = new AnalyzedArticle(task[1] as long)
					def builder = new groovy.json.JsonBuilder()
					builder.nlp {
						id article.id
						synopsis article.synopsis
						keyWords article.keyWords
						keyWordsShort article.keyWordsShort
						similars article.similars
					}
					redis.rpush("queue:article", builder.toString())
					println "${new Date()} - Analyzed and pushed to the queue:article"
				} else {
					continue
				}
			} catch (Exception e) {
				e.printStackTrace()
			}
		}
	}

	static void main(String [] args) {
		println "Starting Natural Language Processor"
		NLProcessor processor = new NLProcessor()

		if (args.size() != 1) {
			System.err.println("Not enought arguments")
			System.exit(1)
		}

		if (args[0] == '--redis-mode') {
			processor.redisMode()
		} 
	}

}
