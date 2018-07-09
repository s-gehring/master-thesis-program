package gehring.uima.distributed;

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

import gehring.uima.distributed.compression.CompressionAlgorithm;
import gehring.uima.distributed.compression.NoCompression;

public class SharedUimaProcessor {

	private JavaSparkContext sparkContext = null;
	private final Logger LOGGER;
	private CompressionAlgorithm casCompression;

	public List<SerializedCAS> readDocuments(final CollectionReader reader,
			final AnalysisEngineDescription pipelineDescription) {
		CAS cas;
		try {
			cas = CasCreationUtils.createCas(pipelineDescription);
		} catch (ResourceInitializationException e1) {
			throw new RuntimeException("Failed to create CAS for document reading.", e1);
		}
		List<SerializedCAS> result = new LinkedList<SerializedCAS>();
		try {
			while (reader.hasNext()) {
				reader.getNext(cas);
				result.add(new SerializedCAS(cas, this.casCompression));
				cas.reset();
			}
		} catch (CollectionException | IOException e) {
			throw new RuntimeException("There was an error collecting all the documents.", e);
		}
		return result;
	}

	public SharedUimaProcessor(final JavaSparkContext sparkConfiguration) {
		this(sparkConfiguration, NoCompression.getInstance(), Logger.getLogger(SharedUimaProcessor.class));
	}

	public SharedUimaProcessor(final JavaSparkContext sparkConfiguration, final CompressionAlgorithm compression) {
		this(sparkConfiguration, compression, Logger.getLogger(SharedUimaProcessor.class));
	}

	public SharedUimaProcessor(final JavaSparkContext sparkConfiguration, final CompressionAlgorithm compression,
			final Logger logger) {
		this.LOGGER = logger;
		this.casCompression = compression;
		this.sparkContext = sparkConfiguration;
	}

	private static int minMax(final int lowerBound, final int target, final int upperBound) {
		if (target > upperBound) {
			return upperBound;
		}
		if (target < lowerBound) {
			return lowerBound;
		}
		return target;
	}

	private static int calculatePartitionNumber(final List<SerializedCAS> underlyingList,
			final JavaSparkContext sparkContext) {
		// Arbitrary values.
		int min = 30, max = 100;

		return minMax(min, underlyingList.size() / 30, max);

	}

	private AnalysisResult processWithContext(final CollectionReaderDescription readerDescription,
			final AnalysisEngineDescription pipelineDescription, final int partitionNum,
			final JavaSparkContext sparkContext) {
		this.LOGGER.info("Preparing to read documents.");
		CollectionReader reader;
		try {
			reader = CollectionReaderFactory.createReader(readerDescription);
		} catch (ResourceInitializationException e) {
			throw new RuntimeException("Error instantiating the collection reader.", e);
		}
		this.LOGGER.info("Prepared document reader. Proceed to actually read...");
		JavaRDD<SerializedCAS> documents;
		List<SerializedCAS> c = this.readDocuments(reader, pipelineDescription);
		if (partitionNum == 0) {
			documents = sparkContext.parallelize(c);
		} else if (partitionNum < 0) {
			documents = sparkContext.parallelize(c, calculatePartitionNumber(c, sparkContext));
		} else {
			documents = sparkContext.parallelize(c, partitionNum);
		}

		this.LOGGER.info(documents.count() + " elements found to be processed.");

		this.LOGGER.info("Elements are partitioned into " + documents.getNumPartitions() + " slices.");

		JavaRDD<SerializedCAS> result = documents.flatMap(new FlatProcess(pipelineDescription));

		AnalysisResult resultWrapper = new AnalysisResult(result, pipelineDescription);

		return resultWrapper;
	}

	public AnalysisResult process(final CollectionReaderDescription readerDescription,
			final AnalysisEngineDescription pipelineDescription) {
		return this.process(readerDescription, pipelineDescription, -1);
	}

	public AnalysisResult process(final CollectionReaderDescription readerDescription,
			final AnalysisEngineDescription pipelineDescription, final int partitionNum) {
		return this.processWithContext(readerDescription, pipelineDescription, partitionNum, this.sparkContext);

	}

}
