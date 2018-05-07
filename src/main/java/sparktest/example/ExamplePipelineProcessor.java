package sparktest.example;

import static org.junit.Assert.fail;

import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.cas.CAS;
import org.apache.uima.collection.CollectionReaderDescription;

import sparktest.SharedUimaProcessor;

public class ExamplePipelineProcessor {

	public static void main(final String[] args) {
		CollectionReaderDescription reader = SampleCollectionReaderFactory.getTestFileReaderDescription();
		AnalysisEngineDescription pipeline = SamplePipelineFactory.getNewPipelineDescription();

		SparkConf configuration = new SparkConf().setMaster("spark://localhost:8001")
				.setAppName(ExamplePipelineProcessor.class.getSimpleName() + " (Spark Example)")
				.set("spark.cores.max", "2").set("spark.executor.memory", "2g");

		SharedUimaProcessor processor = new SharedUimaProcessor(configuration);
		Iterator<CAS> results = processor.process(reader, pipeline);

		if (!results.hasNext()) {
			fail("No results found.");
		}
		int i = 0;
		while (results.hasNext()) {
			CAS currentResult = results.next();
			System.out.println("Result [" + (++i) + "]: " + currentResult);
		}
	}

}
