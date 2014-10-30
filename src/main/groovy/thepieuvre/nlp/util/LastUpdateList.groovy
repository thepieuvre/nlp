package thepieuvre.nlp.util

class LastUpdateList {

	private static final String INDEX = 'LASTUPDATE'
	long expire // in seconds

	LastUpdateList(long expire) {
		this.expire = expire
	}

    boolean containsKey(key, redis) {
    	redis.get("$INDEX:$key")
    }

    void put(key, redis) {
    	redis.set("$INDEX:$key", "$key", 'NX', 'EX', expire)
    }
	
}