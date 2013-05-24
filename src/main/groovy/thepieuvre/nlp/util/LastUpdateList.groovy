package thepieuvre.nlp.util

class LastUpdateList {

	Set set = new TreeSet<Long>()
	def list = []
	int fixedSize

	LastUpdateList(int fixedSize) {
		this.fixedSize = fixedSize
	}
	
	def add(elem) {
		if (list.size() > fixedSize) {
			set.remove(list.last())
			list = list.tail()
		}
		list << elem
		set.add(elem)
	}

	def hasElem(elem) {
		set.contains(elem)
	}

	String toString() {
		list.toString()
	}
}