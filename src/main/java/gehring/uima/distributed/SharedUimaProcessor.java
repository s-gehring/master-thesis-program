package gehring.uima.distributed;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
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

	private SparkConf sparkConfiguration;
	private final Logger LOGGER;

	public static List<SerializedCAS> readDocuments(final CollectionReader reader, final JavaSparkContext sparkContext,
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
				result.add(new SerializedCAS(cas));
				cas.reset();
			}
		} catch (CollectionException | IOException e) {
			throw new RuntimeException("There was an error collecting all the documents.", e);
		}
		return result;
	}

	public SharedUimaProcessor(final SparkConf sparkConfiguration) {
		this(sparkConfiguration, Logger.getLogger(SharedUimaProcessor.class));
	}

	public SharedUimaProcessor(final SparkConf sparkConfiguration, final Logger logger) {
		this.LOGGER = logger;
		this.sparkConfiguration = sparkConfiguration;
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

	public Iterator<CAS> process(final CollectionReaderDescription readerDescription,
			final AnalysisEngineDescription pipelineDescription) {
		return this.process(readerDescription, pipelineDescription, -1);
	}

	public Iterator<CAS> process(final CollectionReaderDescription readerDescription,
			final AnalysisEngineDescription pipelineDescription, final int partitionNum) {
		Iterator<SerializedCAS> serializedResultIterator;
		List<SerializedCAS> collectedResults;

		try (JavaSparkContext sparkContext = new JavaSparkContext(this.sparkConfiguration)) {
			this.LOGGER.info("Preparing to read documents.");
			CollectionReader reader;
			try {
				reader = CollectionReaderFactory.createReader(readerDescription);
			} catch (ResourceInitializationException e) {
				throw new RuntimeException("Error instantiating the collection reader.", e);
			}
			this.LOGGER.info("Prepared document reader. Proceed to actually read...");
			JavaRDD<SerializedCAS> documents;
			List<SerializedCAS> c = readDocuments(reader, sparkContext, pipelineDescription);
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

			this.LOGGER.info(result.count() + " elements processed.");
			collectedResults = result.collect();
			this.LOGGER.info(collectedResults.size() + " elements retrieved.");
			serializedResultIterator = result.collect().iterator();
		}
		return new CASIterator(serializedResultIterator, pipelineDescription);
	}

}
