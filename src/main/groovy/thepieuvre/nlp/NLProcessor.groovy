package thepieuvre.nlp

import thepieuvre.nlp.util.LastUpdateList

import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import redis.clients.jedis.JedisPoolConfig

import groovy.json.JsonBuilder

import groovy.util.logging.Log4j

import org.apache.log4j.PropertyConfigurator

@Log4j
class NLProcessor {

	JedisPool pool

	private LastUpdateList updated = new LastUpdateList(1000)

	private Set toProcess = new TreeSet<Long>()

	private static def cli

	static {
  		cli = new CliBuilder(
			usage: 'nlp [options] [queue name]\n\tqueue name: name of the queue (optional)',
        	header: 'The Pieuvre - NLP: Natural Language Processor',
        	stopAtNonOption: false
    	)
		cli.h longOpt: 'help', 'print this message'
		cli.c longOpt: 'config', args:1, argName:'configPath', 'configuration file\'s path'
		cli._ longOpt: 'redis-host', args:1, argName:'redisHost', 'redis server\'s hostname'
		cli._ longOpt: 'redis-port', args:1, argName:'redisPort', 'redis server\'s port'
		cli._ longOpt: 'redis-url', args:1, argName:'redisUrl', 'redis server\'s url -- server.domain.com:456'
	}

	NLProcessor(String redisHost, int redisPort) {
		JedisPoolConfig config = new JedisPoolConfig()
		config.setTestOnBorrow(true)
		pool = new JedisPool(config, redisHost, redisPort, 180000)
	}


	private def updatingSimilars(Jedis redis, def similars) {
		log.info "Updating similars"
		similars.each { id, score ->
			if (! updated.containsKey(id) && ! toProcess.contains(id)) {
				redis.rpush("queue:nlp-low", "$id")
				updated.put(id, 0b0)
 			}
		}
	}

	def redisMode(String queue) {
		log.info "Starting listenning to the $queue"
		while (true) {
			Jedis redis = pool.getResource()
			try {
				int count = 0
				def task = redis.blpop(31415, queue)
				if (queue == 'queue:nlp-low') {
					while(toProcess.size() < 100) {
						log.info "depoped: $task"
						count++
						if (task) {
							String elem = ((task instanceof ArrayList)? task[1]:task)
							toProcess.add(elem.toLong())
							log.info "Added $elem for Processing ${toProcess.size()} on  ${count}"
						} else {
							log.info "no more task"
							break
						}
						task = redis.lpop(queue)
					}
				} else {
					if (task) {
						toProcess << (task[1] as long)
					}
				}
				log.debug "To process ready to be processed"

				toProcess.each {
					log.info ">>>>> $it"
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
					if (queue != 'queue:nlp-low') {
						updatingSimilars(redis, article.similars)
					}
					log.info "Analyzed and pushed to the queue:article"
				}
				toProcess.clear()
				count = 0
			} catch (Exception e) {
				log.error e
				e.printStackTrace()
			} finally {
				pool.returnResource(redis)
			}
		}
	}

	private static Map parsingCli(String [] args) {
		def opts = cli.parse(args)

		if (opts.h) {
			cli.usage()
			System.exit(0)
		}

		if (opts.arguments().size() > 1) {
			System.err.&println 'Not enought arguments'
			cli.usage()
			System.exit(1)
		}

		def parsed = [:]

		if (opts.arguments().size() == 1) {
			parsed.queue = opts.arguments()[0]
		}

		parsed.redisHost = 'localhost'
		parsed.redisPort = 6379

		if (opts.'redis-url') {
			URI uri = new URI(opts.'redis-url')
			parsed.redisHost = uri.getHost()
			parsed.redisPort = uri.getPort()
		}

		if (opts.'redis-host') {
			parsed.redisHost = opts.'redis-host'
		}

		if (opts.'redis-port') {
			parsed.redisPort = opts.'redis-port' as int
		}

		if (opts.c) {
			def config = new ConfigSlurper().parse(new File(opts.c).toURL())
			PropertyConfigurator.configure(config.toProperties())
		}

		return parsed
	}

	static void main(String [] args) {
		def params = parsingCli(args)

		NLProcessor processor = new NLProcessor(params.redisHost, params.redisPort)

		RedisHelper.init(processor.pool)

		String queue = (params.queue)?"queue:${params.queue}":'queue:nlp'
		processor.redisMode(queue)
	}

	void finalize() throws Throwable {
		pool.destroy()
	}
}
