package thepieuvre.nlp

import thepieuvre.nlp.util.LastUpdateList

import redis.clients.jedis.Jedis

import groovy.json.JsonBuilder

class NLProcessor {

	private Jedis redis = new Jedis("localhost")

	private LastUpdateList updated = new LastUpdateList(1000)

	private Set toProcess = new TreeSet<Long>()

	private def updatingSimilars(Jedis redis, def similars) {
		similars.each { id, score ->
			if (! updated.hasElem(id) && ! toProcess.contains(id)) {
				redis.rpush("queue:nlp-low", "$id")
				updated.add(id)
				println "..... $id pushed for updating"
 			} else {
 				println "..... $id not repushed"
 			}
		}
	}

	def redisMode(String queue) {
		println "Starting listenning to the $queue"
		redis.sadd('queues', queue)
		while (true) {
			try {
				int count = 0
				def task = redis.blpop(31415, queue)
				if (queue == 'queue:nlp-low') {
					while(toProcess.size() < 1000 || task) {
						count++
						if (task) {
							toProcess << (((task.size() == 1)?task[0]:task[1]) as long)
							println "To Process ${toProcess.size()} on  ${count}"
						} else {
							println "no more task"
							break
						}
						task = redis.lpop(queue)
						println "depoped: $task"
					}
				} else {
					if (task) {
						toProcess << (task[1] as long)
					}
				}
				println "To process ready to be processed"
				
				toProcess.each {
					println "${new Date()}.>>>>> $it"
					AnalyzedArticle article = new AnalyzedArticle(it)
					def builder = new JsonBuilder()
					builder.nlp {
						id article.id
						synopsis article.synopsis
						keyWords article.keyWords
						keyWordsShort article.keyWordsShort
						similars article.similars
					}
					redis.rpush("queue:article", builder.toString())
					updatingSimilars(redis, article.similars )
					println "${new Date()} - Analyzed and pushed to the queue:article"
				}
				toProcess.clear()
				count = 0
			} catch (Exception e) {
				e.printStackTrace()
			}
		}
	}

	static void main(String [] args) {
		println "Starting Natural Language Processor"
		NLProcessor processor = new NLProcessor()

		if (args.size() < 1) {
			System.err.println("Not enought arguments")
			System.exit(1)
		}

		if (args[0] == '--redis-mode') {
			def queue = (args.size() == 2 && args[1] == 'low')?'queue:nlp-low':'queue:nlp'
			processor.redisMode(queue)
		} 
	}

}
