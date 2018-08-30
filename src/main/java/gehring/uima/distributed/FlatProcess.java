package gehring.uima.distributed;

import gehring.uima.distributed.exceptions.UimaException;

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

    private static final long         serialVersionUID = -7768952669222098016L;
    private AnalysisEngineDescription pipelineDescription;
    private static AnalysisEngine     pipeline;

    private AnalysisEngine preparePipeline() {
        try {
            return AnalysisEngineFactory.createEngine(this.pipelineDescription);
        } catch (ResourceInitializationException e) {
            throw new UimaException("Failed to initialize pipeline.", e);
        }
    }

    public FlatProcess(final AnalysisEngineDescription engineDescription) {
        this.pipelineDescription = engineDescription;

    }

    private static synchronized void setPipeline(final AnalysisEngine engine) {
        pipeline = engine;
    }

    @Override
    public Iterator<SerializedCAS> call(final SerializedCAS inputCAS) throws Exception {
        if (pipeline == null) {
            setPipeline(this.preparePipeline());
        }
        CAS cas = pipeline.newCAS();
        inputCAS.populateCAS(cas);

        pipeline.process(cas);
        Collection<SerializedCAS> casCollection = new LinkedList<>();

        casCollection.add(new SerializedCAS(cas));
        return casCollection.iterator();
    }

}
