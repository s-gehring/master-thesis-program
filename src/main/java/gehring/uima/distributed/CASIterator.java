package gehring.uima.distributed;

import java.util.Iterator;
import java.util.LinkedList;

import org.apache.log4j.Logger;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.cas.CAS;

public class CASIterator implements Iterator<CAS> {

	private static final Logger LOGGER = Logger.getLogger(CASIterator.class);
	private Iterator<CAS> underlyingIterator;

	private LinkedList<CAS> underlyingList = new LinkedList<>();

	protected CASIterator(final Iterator<SerializedCAS> underlyingIterator,
			final AnalysisEngineDescription pipelineDescription) {

		if (underlyingIterator == null) {
			throw new NullPointerException("Provided sCAS iterator is null.");
		}
		if (pipelineDescription == null) {
			throw new NullPointerException("Provided pipeline description is null.");
		}
		LOGGER.debug("Trying to extract CAS.");
		int counter = 0;
		while (underlyingIterator.hasNext()) {
			SerializedCAS sCas = underlyingIterator.next();
			this.underlyingList.add(sCas.getCAS(pipelineDescription));
			counter++;
		}
		this.underlyingIterator = this.underlyingList.iterator();
		LOGGER.debug("Successfully extracted " + counter + " CAS.");

	}

	@Override
	public boolean hasNext() {
		return this.underlyingIterator.hasNext();
	}

	@Override
	public CAS next() {
		return this.underlyingIterator.next();
	}

}
