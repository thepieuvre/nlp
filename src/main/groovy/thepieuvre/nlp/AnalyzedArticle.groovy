package thepieuvre.nlp

class AnalyzedArticle {

	long id

	private def nGrams =[]

	def keyWords = []
	def keyWordsShort = []

	def similars= []

	String synopsis

	RedisHelper redis = new RedisHelper()

	AnalyzedArticle(long id){
		this.id = id
		nGrams = redis.getNGram(id)
		//synopsis = synopsis()
		keyWords = nGrams*.name
		keyWordsShort = keyWordsShorter()
		similars = similars()
	}
	
	def keyWordsShorter() {
		if (keyWords.size() != 0) {
			keyWords[0..((keyWords.size() < 15)?keyWords.size()-1:14)]
		} else {
			keyWords
		}
	}

	def synopsis() {
		def synopsis = []
		def parts = redis.getTrainedGram(id)
		parts*.name.each { part ->
			nGrams*.name.each { keyWord ->
				def cleaned = part.replaceAll('\\p{Punct}', '')
				if (cleaned.matches("${keyWord.replaceAll('\\p{Punct}', '').split('\\s').join('.*')}")) {
					synopsis.add(cleaned)
				}
			}
		}
		return synopsis.join(' [...] ')
	}

	def similars() {
		def all = mergingAll()
		def metrics = metrics(all)
		long upper = metrics.max 
		long stdDev = metrics.stdDev
		long lower = upper - stdDev
		def res = [:]
		all.each { k, v ->
			if (v <= upper && v >= lower)
				res[k] = v
		}
		res = res.sort { a, b -> b.value <=> a.value}
		return res
	}

	private def metrics(def articles) {
		def metrics = [:]
		long max = 0
		long average = 0
		long sumSq = 0
		long counter = 0
		articles.each {
			// max
			if (it.value > max)
				max = it.value
			// average
			average += it.value
			// Std Dev
			sumSq += it.value * it.value
			counter++
		}
		metrics.max = max
		metrics.average = (counter > 0)?(average/counter):0
		metrics.stdDev = (counter > 0)?((sumSq/counter - (average/counter)**2)**0.5):0
		return metrics
	}

	private def mergingAll() {
		//def unigram = redis.getUniGram(id)
		//def bigram = redis.getBiGram(id)
		//def trainedgram = getTrainedGram(id)

		def merged = [:]
		// unigram.each {
		// 	int score = it.score as int
		// 	it.articles.each { art ->
		// 		if(merged[art]) {
		// 			merged[art] = merged[art] + score
		// 		} else {
		// 			merged[art] = score
		// 		}
		// 	}
		// }
		// bigram.each {
		// 	int score = it.score as int
		// 	it.articles.each { art ->
		// 		if(merged[art]) {
		// 			merged[art] = merged[art] + score
		// 		} else {
		// 			merged[art] = score
		// 		}
		// 	}
		// }
		nGrams.each {
			int score = it.score as int
			it.articles.each { art ->
				if(merged[art]) {
					merged[art] = merged[art] + score
				} else {
					merged[art] = score
				}
			}
		}
		// trainedgram.each {
		// 	int score = it.score as int
		// 	it.articles.each { art ->
		// 		if(merged[art]) {
		// 			merged[art] = merged[art] + score
		// 		} else {
		// 			merged[art] = score
		// 		}
		// 	}
		// }

		return merged
	}
}