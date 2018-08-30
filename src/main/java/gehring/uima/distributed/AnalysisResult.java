package gehring.uima.distributed;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.cas.CAS;

public class AnalysisResult implements Serializable {

    private static final long                 serialVersionUID = -7800008391242232981L;
    protected final JavaRDD<SerializedCAS>    result;
    protected final AnalysisEngineDescription pipelineDescription;

    protected AnalysisResult(final JavaRDD<SerializedCAS> result,
            final AnalysisEngineDescription pipeline) {
        if (result == null) {
            throw new NullPointerException("Result RDD is null.");
        }
        if (pipeline == null) {
            throw new NullPointerException("Result cannot be deserialized. Pipeline is null.");
        }

        this.result = result;
        this.pipelineDescription = pipeline;
    }

    public int getNumPartitions() {
        return this.result.getNumPartitions();
    }

    public List<CAS> collect() {
        int numPartitions = this.result.getNumPartitions();
        int[] partitionIds = new int[numPartitions];
        for (int i = 0; i < numPartitions; ++i) {
            partitionIds[i] = i;
        }
        return this.collectPartitions(partitionIds);
    }

    public List<CAS> collectPartitions(final int[] partitionIds) {
        List<SerializedCAS>[] serializedResult = this.result.collectPartitions(partitionIds);
        if (serializedResult.length == 0) {
            return new LinkedList<>();
        }
        // A rough estimate on the resulting list size.
        List<CAS> result = new ArrayList<>(serializedResult.length * serializedResult[0].size());
        for (List<SerializedCAS> curPartition : serializedResult) {
            for (SerializedCAS sCas : curPartition) {
                result.add(sCas.getCAS(this.pipelineDescription));
            }
        }
        return result;
    }

    public long count() {
        return this.result.count();
    }

    public void saveAsXmi(final String filepath) {
        JavaRDD<XmiSerializedCAS> deserialized = this.result
                .flatMap(new FlatMapFunction<SerializedCAS, XmiSerializedCAS>() {

                    private static final long serialVersionUID = 6948383260570715934L;

                    @Override
                    public Iterator<XmiSerializedCAS> call(final SerializedCAS sCas)
                            throws Exception {
                        LinkedList<XmiSerializedCAS> result = new LinkedList<>();
                        result.add(new XmiSerializedCAS(sCas,
                                AnalysisResult.this.pipelineDescription));
                        return result.iterator();
                    }

                });

        deserialized.saveAsTextFile(filepath);
    }

}
