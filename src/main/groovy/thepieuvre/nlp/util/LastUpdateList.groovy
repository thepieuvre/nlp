package thepieuvre.nlp.util

class LastUpdateList extends LinkedHashMap {

	int fixedSize

	LastUpdateList(int fixedSize) {
		this.fixedSize = fixedSize
	}

	protected boolean removeEldestEntry(Map.Entry eldest) {
        return size() > fixedSize;
    }
	
}