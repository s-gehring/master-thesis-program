package sparktest;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.cas.CAS;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.resource.ResourceInitializationException;

public class FlatProcess implements FlatMapFunction<SerializedCAS, SerializedCAS> {

	private static AnalysisEngine pipeline;

	public FlatProcess(final AnalysisEngineDescription engineDescription) {

		try {
			pipeline = AnalysisEngineFactory.createEngine(engineDescription);
		} catch (ResourceInitializationException e) {
			throw new RuntimeException("Failed to initialize pipeline.", e);
		}
	}

	@Override
	public Iterator<SerializedCAS> call(final SerializedCAS inputCAS) throws Exception {
		CAS cas = FlatProcess.pipeline.newCAS();
		inputCAS.populateCAS(cas);

		FlatProcess.pipeline.process(cas);
		Collection<SerializedCAS> casCollection = new LinkedList<SerializedCAS>();

		casCollection.add(new SerializedCAS(cas));
		return casCollection.iterator();
	}

}
