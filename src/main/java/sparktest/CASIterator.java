package sparktest;

import java.util.Iterator;

import org.apache.uima.cas.CAS;

public class CASIterator implements Iterator<CAS> {

	private Iterator<SerializedCAS> underlyingIterator;

	protected CASIterator(final Iterator<SerializedCAS> underlyingIterator) {
		this.underlyingIterator = underlyingIterator;
	}

	@Override
	public boolean hasNext() {
		return this.underlyingIterator.hasNext();
	}

	@Override
	public CAS next() {
		return this.underlyingIterator.next().getCAS();
	}

}
