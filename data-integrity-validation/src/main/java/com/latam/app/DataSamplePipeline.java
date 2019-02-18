package com.latam.app;

import com.google.cloud.MonitoredResource;
import com.google.cloud.logging.LogEntry;
import com.google.cloud.logging.Logging;
import com.google.cloud.logging.LoggingOptions;
import com.google.cloud.logging.Payload.JsonPayload;
import com.google.cloud.logging.Severity;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.beam.sdk.extensions.joinlibrary.Join;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;


public class DataSamplePipeline {


	private static final Logger LOG = LoggerFactory.getLogger(DataSamplePipeline.class);
	private static LoggingOptions options = LoggingOptions.getDefaultInstance();
	
	
	public interface CustomOptions extends PipelineOptions {

		@Description("GCS location of sample file")
		@Required
		ValueProvider<String> getSampleFilepattern();
		void setSampleFilepattern(ValueProvider<String> value);

		@Description("GCS location of input file")
		@Required
		ValueProvider<String> getInputFilepattern();
		void setInputFilepattern(ValueProvider<String> value);


		@Description("Expected row count")
		@Required
		ValueProvider<Integer> getExpectedResult();
		void setExpectedResult(ValueProvider<Integer> value);
		
		@Description("Log Information")
		@Required
		ValueProvider<String> getLogInfo();
		void setLogInfo(ValueProvider<String> value);

	}

	static class CreateKeyPairs extends DoFn<String, KV<String,String>> {

		@ProcessElement
		public void processElement(ProcessContext c) {
			c.output(KV.of(c.element(), ""));
		}
	}
	
	static class CompareResults extends DoFn<Long, Boolean> {
		ValueProvider<String> logInfo;
		ValueProvider<Integer> expected;
		
		CompareResults(ValueProvider<Integer> expectedResult, ValueProvider<String> loginfo) {
			this.expected = expectedResult;
			this.logInfo=loginfo;
		}
		
		@ProcessElement
		public void processElement(@Element Long result, OutputReceiver<Boolean> out, ProcessContext c) {
			CustomLogger.WriteLog(logInfo.get(), result.toString() + " de " + expected.get() + " registros muestreados coinciden con los extraidos (" + result*100/expected.get() + "%).", "DataSamplePipeline", Severity.INFO);
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException {
		PipelineOptionsFactory.register(CustomOptions.class);
		CustomOptions customOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(CustomOptions.class);
		customOptions.setJobName("sampleValidation" + new Date().getTime());

		Pipeline p = Pipeline.create(customOptions);

		PCollection<String> sample = p.apply("Read Sample", TextIO.read().from(customOptions.getSampleFilepattern()));
		PCollection<String> extracted = p.apply("Read File", TextIO.read().from(customOptions.getInputFilepattern()));

		final PCollectionView<Long> countSample = sample.apply("Count values", Count.globally()).apply(View.asSingleton());

		PCollection<KV<String, String>> sampleKV = sample.apply("Sample to KP", ParDo.of(new CreateKeyPairs()));
		PCollection<KV<String, String>> extractedKV = extracted.apply("Extracted to KP", ParDo.of(new CreateKeyPairs()));

		PCollection<KV<String, KV<String, String>>> joined = Join.innerJoin(sampleKV, extractedKV);

		PCollection<Long> count = joined.apply("Count join results", Count.globally());

		PCollection<Boolean> result = count.apply("Compare results", ParDo.of(new CompareResults(customOptions.getExpectedResult(), customOptions.getLogInfo())).withSideInputs(countSample));

		p.run();
	}
}