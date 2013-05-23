package thepieuvre.nlp.util

class LastUpdateList {

	def list = []
	int fixedSize

	LastUpdateList(int fixedSize) {
		this.fixedSize = fixedSize
	}

	def add(elem) {
		if (list.size() > fixedSize) {
			list = list.tail()
		}
		list << elem
	}

	def hasElem(elem) {
		list.find() {
			it == elem
		}
	}
}