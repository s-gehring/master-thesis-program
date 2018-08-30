package gehring.uima.distributed;

import gehring.uima.distributed.compression.CompressionAlgorithm;
import gehring.uima.distributed.compression.NoCompression;
import gehring.uima.distributed.exceptions.UimaException;
import gehring.uima.distributed.serialization.CasSerialization;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.cas.CAS;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.collection.CollectionReaderDescription;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.CasCreationUtils;

public class SharedUimaProcessor {

    private JavaSparkContext     sparkContext = null;
    private final Logger         logger;
    private CompressionAlgorithm casCompression;
    private CasSerialization     casSerialization;

    public List<SerializedCAS> readDocuments(final CollectionReader reader,
            final AnalysisEngineDescription pipelineDescription) {
        CAS cas;
        try {
            cas = CasCreationUtils.createCas(pipelineDescription);
        } catch (ResourceInitializationException e1) {
            throw new UimaException("Failed to create CAS for document reading.", e1);
        }
        List<SerializedCAS> result = new LinkedList<>();
        try {
            while (reader.hasNext()) {
                reader.getNext(cas);
                result.add(new SerializedCAS(cas, this.casCompression, this.casSerialization));
                cas.reset();
            }
        } catch (CollectionException | IOException e) {
            throw new UimaException("There was an error collecting all the documents.", e);
        }
        return result;
    }

    public SharedUimaProcessor(final JavaSparkContext sparkConfiguration) {
        this(sparkConfiguration, NoCompression.getInstance(),
                Logger.getLogger(SharedUimaProcessor.class));
    }

    public SharedUimaProcessor(final JavaSparkContext sparkConfiguration,
            final CompressionAlgorithm compression) {
        this(sparkConfiguration, compression, Logger.getLogger(SharedUimaProcessor.class));
    }

    public SharedUimaProcessor(final JavaSparkContext sparkConfiguration,
            final CompressionAlgorithm compression,
            final Logger logger) {
        this.logger = logger;
        this.casCompression = compression;
        this.sparkContext = sparkConfiguration;
    }

    @SuppressWarnings("unused")
    private static int calculatePartitionNumber(final List<SerializedCAS> underlyingList,
            final JavaSparkContext sparkContext) {
        return underlyingList.size();

    }

    private AnalysisResult processWithContext(final CollectionReaderDescription readerDescription,
            final AnalysisEngineDescription pipelineDescription, final int partitionNum,
            final JavaSparkContext sparkContext) {
        this.logger.info(
                "Preparing to read documents. (Partition argument is '" + partitionNum + "')");
        CollectionReader reader;
        try {
            reader = CollectionReaderFactory.createReader(readerDescription);
        } catch (ResourceInitializationException e) {
            throw new UimaException("Error instantiating the collection reader.", e);
        }
        this.logger.trace("Prepared document reader. Proceed to actually read...");
        JavaRDD<SerializedCAS> documents;
        List<SerializedCAS> c = this.readDocuments(reader, pipelineDescription);
        if (c.isEmpty()) {
            this.logger.error("No documents to analyze.");
            return null;
        }
        if (partitionNum == 0) {
            documents = sparkContext.parallelize(c);
        } else if (partitionNum < 0) {
            documents = sparkContext.parallelize(c, calculatePartitionNumber(c, sparkContext));
        } else {
            documents = sparkContext.parallelize(c, partitionNum);
        }

        this.logger.info(documents.count() + " elements found to be processed.");

        this.logger
                .info("Elements are partitioned into " + documents.getNumPartitions() + " slices.");

        JavaRDD<SerializedCAS> result = documents.flatMap(new FlatProcess(pipelineDescription));

        return new AnalysisResult(result, pipelineDescription);

    }

    public AnalysisResult process(final CollectionReaderDescription readerDescription,
            final AnalysisEngineDescription pipelineDescription) {
        return this.process(readerDescription, pipelineDescription, -1);
    }

    public AnalysisResult process(final CollectionReaderDescription readerDescription,
            final AnalysisEngineDescription pipelineDescription, final int partitionNum) {
        return this.processWithContext(readerDescription, pipelineDescription, partitionNum,
                this.sparkContext);

    }

}
