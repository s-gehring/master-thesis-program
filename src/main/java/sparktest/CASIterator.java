package sparktest;

import java.util.Iterator;

import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.cas.CAS;

public class CASIterator implements Iterator<CAS> {

	private Iterator<SerializedCAS> underlyingIterator;
	private AnalysisEngineDescription pipelineDescription;

	protected CASIterator(final Iterator<SerializedCAS> underlyingIterator,
			final AnalysisEngineDescription pipelineDescription) {
		this.underlyingIterator = underlyingIterator;
		this.pipelineDescription = pipelineDescription;
		if (underlyingIterator == null) {
			throw new NullPointerException("Provided sCAS iterator is null.");
		}
		if (pipelineDescription == null) {
			throw new NullPointerException("Provided pipeline description is null.");
		}
	}

	@Override
	public boolean hasNext() {
		return this.underlyingIterator.hasNext();
	}

	@Override
	public CAS next() {
		return this.underlyingIterator.next().getCAS(this.pipelineDescription);
	}

}
