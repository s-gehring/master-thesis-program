package sparktest.example;

import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.cas.CAS;
import org.apache.uima.collection.CollectionReaderDescription;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import sparktest.SharedUimaProcessor;
import sparktest.benchmark.BenchmarkResult;
import sparktest.benchmark.Benchmarks;

public class ExamplePipelineProcessor {

	private static void examplePipeline() {
		CollectionReaderDescription reader = SampleCollectionReaderFactory.getSampleTextReaderDescription();
		AnalysisEngineDescription pipeline = SamplePipelineFactory.getNewPipelineDescription();

		SparkConf configuration = new SparkConf().setMaster("spark://master:7077")
				.setAppName(ExamplePipelineProcessor.class.getSimpleName() + " (Spark Example)")
				.set("spark.cores.max", "2").set("spark.executor.memory", "2g")
		/* .set("spark.submit.deployMode", "cluster") */;

		SharedUimaProcessor processor = new SharedUimaProcessor(configuration);
		Iterator<CAS> results = processor.process(reader, pipeline);

		if (!results.hasNext()) {
			System.exit(1);
		}
		int i = 0;
		while (results.hasNext()) {
			CAS currentResult = results.next();
			System.out.println("Result [" + (++i) + "]: " + currentResult);
		}
	}

	private static void printBenchmark(final CollectionReaderDescription reader,
			final AnalysisEngineDescription pipeline) {

		SparkConf configuration = new SparkConf().setMaster("spark://master:7077")
				.setAppName(ExamplePipelineProcessor.class.getSimpleName() + " (Spark Example)")
				.set("spark.cores.max", "2").set("spark.executor.memory", "2g");

		BenchmarkResult result = Benchmarks.benchmark(reader, pipeline, configuration);

		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		JsonParser jp = new JsonParser();
		JsonElement je = jp.parse(result.toString());
		String prettyJsonString = gson.toJson(je);

		System.out.println(prettyJsonString);
	}

	public static void main(final String[] args) {
		CollectionReaderDescription reader;
		AnalysisEngineDescription pipeline;

		reader = SampleCollectionReaderFactory.getSampleTextReaderDescription();
		pipeline = SamplePipelineFactory.getNewPipelineDescription();

		// printBenchmark(reader, pipeline);

		reader = SampleCollectionReaderFactory.getGutenbergReaderDescription();
		pipeline = SamplePipelineFactory.getNewPipelineDescription();

		printBenchmark(reader, pipeline);

	}

}
