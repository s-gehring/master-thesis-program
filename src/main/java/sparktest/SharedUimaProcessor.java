package sparktest;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.CASException;
import org.apache.uima.cas.admin.CASFactory;
import org.apache.uima.cas.admin.CASMgr;
import org.apache.uima.cas.impl.CASImpl;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.collection.CollectionReaderDescription;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.apache.uima.resource.ResourceInitializationException;

public class SharedUimaProcessor {

	private SparkConf sparkConfiguration;

	public static JavaRDD<SerializedCAS> readDocuments(final CollectionReader reader,
			final JavaSparkContext sparkContext) {
		CASMgr casMgr = CASFactory.createCAS();
		CASImpl casImplementation = (CASImpl) casMgr;
		casImplementation.commitTypeSystem();

		try {
			// TODO Maybe reuse CAS? It's guaranteed to be single threaded here.
			casMgr.initCASIndexes();
		} catch (CASException e1) {
			throw new RuntimeException("Error creating initial CAS indexes.", e1);
		}

		CAS cas = casImplementation;
		casMgr = null;
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
		return sparkContext.parallelize(result);
	}

	public SharedUimaProcessor(final SparkConf sparkConfiguration) {
		this.sparkConfiguration = sparkConfiguration;
	}

	public Iterator<CAS> process(final CollectionReaderDescription readerDescription,
			final AnalysisEngineDescription pipeline) {
		Iterator<SerializedCAS> serializedResultIterator;
		try (JavaSparkContext sparkContext = new JavaSparkContext(this.sparkConfiguration)) {

			CollectionReader reader;
			try {
				reader = CollectionReaderFactory.createReader(readerDescription);
			} catch (ResourceInitializationException e) {
				throw new RuntimeException("Error instantiating the collection reader.", e);
			}

			JavaRDD<SerializedCAS> documents = readDocuments(reader, sparkContext);
			JavaRDD<SerializedCAS> result = documents.flatMap(new FlatProcess(pipeline));

			serializedResultIterator = result.collect().iterator();
		}
		return new CASIterator(serializedResultIterator);
	}

}
