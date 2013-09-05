package thepieuvre.nlp

import thepieuvre.nlp.util.LastUpdateList

import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import redis.clients.jedis.JedisPoolConfig

import groovy.json.JsonBuilder

class NLProcessor {

	static JedisPool pool = new JedisPool(new JedisPoolConfig(), "localhost", 6379, 30000)

	private LastUpdateList updated = new LastUpdateList(1000)

	private Set toProcess = new TreeSet<Long>()

	private def updatingSimilars(Jedis redis, def similars) {
		similars.each { id, score ->
			if (! updated.hasElem(id) && ! toProcess.contains(id)) {
				redis.rpush("queue:nlp-low", "$id")
				updated.add(id)
 			}
		}
	}

	def redisMode(String queue) {
		println "Starting listenning to the $queue"
		while (true) {
			Jedis redis = pool.getResource()
			try {
				int count = 0
				def task = redis.blpop(31415, queue)
				if (queue == 'queue:nlp-low') {
					while(toProcess.size() < 100) {
						count++
						if (task) {
							toProcess << (((task.size() == 1)?task[0]:task[1]) as long)
							println "${new Date()} - To Process ${toProcess.size()} on  ${count}"
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
				println "${new Date()} - To process ready to be processed"
				
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
			} finally {
				NLProcessor.pool.returnResource(redis)
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

	void finalize() throws Throwable {
		pool.destroy()
	}
}
