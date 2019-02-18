package com.latam.app;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

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
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.MonitoredResource;
import com.google.cloud.logging.LogEntry;
import com.google.cloud.logging.Logging;
import com.google.cloud.logging.LoggingOptions;
import com.google.cloud.logging.Payload.JsonPayload;
import com.google.cloud.logging.Severity;


public class RowCountPipeline {


	private static final Logger LOG = LoggerFactory.getLogger(RowCountPipeline.class);
	private static LoggingOptions options = LoggingOptions.getDefaultInstance();
	
	
	public interface CustomOptions extends PipelineOptions {

		@Description("GCS location of input file")
		@Required
		ValueProvider<String> getInputFilepattern();
		void setInputFilepattern(ValueProvider<String> value);


		@Description("Expected row count")
		@Required
		ValueProvider<Integer> getExpectedResult();
		void setExpectedResult(ValueProvider<Integer> value);
		
		
		@Description("Delimiter")
		@Required
		ValueProvider<String> getDelimiter();
		void setDelimiter(ValueProvider<String> value);
		
		@Description("Log Information")
		@Required
		ValueProvider<String> getLogInfo();
		void setLogInfo(ValueProvider<String> value);

	}
	
	
	static class SplitLines extends DoFn<String, ArrayList<String>> {
		ValueProvider<String> delimiter;
	

		SplitLines(ValueProvider<String> delim) {
			this.delimiter = delim;
			
		}

		@ProcessElement
		public void processElement(ProcessContext c) {
			String line = c.element();
			
			ArrayList<String> values = new ArrayList<String>(Arrays.asList(line.split("\\" + delimiter.get(), -1)));
			c.output(values);
		}
	}
	
	static class FilterField extends DoFn<ArrayList<String>, String> {
		ValueProvider<Integer> columnIndex;
		
		@ProcessElement
		public void processElement(@Element ArrayList<String> line, OutputReceiver<String> out) {
			out.output(line.get(0));
		}
	}
	
	static class CompareResults extends DoFn<Long, Boolean> {
		ValueProvider<String> logInfo;
		ValueProvider<Integer> expected;
		//DecimalFormat df2 = new DecimalFormat(".##");
		
		CompareResults(ValueProvider<Integer> expectedResult, ValueProvider<String> loginfo) {
			this.expected = expectedResult;
			this.logInfo=loginfo;
			//this.df2.setRoundingMode(RoundingMode.UP);
		}
		
		@ProcessElement
		public void processElement(@Element Long result, OutputReceiver<Boolean> out) {
			//String roundedResult = df2.format(result.doubleValue());
			//LOG.info("Original value " + result.toString() + " rounded to " + roundedResult);
			if(expected.get().toString().equals(result.toString())) {
				LOG.info("The Count String " + result.toString() + " match the expected value of " + expected.get().toString());
				CustomLogger.WriteLog(logInfo.get(), 
						"The result of the count is valid. Result: " + result.toString() + " / Expected: " + expected.get().toString(),
						"RowCountPipeline",
						Severity.INFO);
			}else {
				LOG.info("The result of the count " + result.toString() + " does not match the expected value of " + expected.get().toString());
				CustomLogger.WriteLog(logInfo.get(), 
						"The result of the count " + result.toString() + " does not match the expected value of " + expected.get().toString(),
						"RowCountPipeline",
						Severity.ERROR);
				throw new java.lang.Error("The result of the count " + result.toString() + " does not match the expected value of " + expected.get().toString());
			
			}
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException {
		PipelineOptionsFactory.register(CustomOptions.class);
		CustomOptions customOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(CustomOptions.class);
		customOptions.setJobName("rowcountvalidation" + new Date().getTime());

		Pipeline p = Pipeline.create(customOptions);
		p.apply("Read File", 
				TextIO.read()
				.from(customOptions.getInputFilepattern()))
		.apply("Split file lines",
				ParDo.of(new SplitLines(customOptions.getDelimiter())))
		.apply("Filter field",
					ParDo.of(new FilterField()))
		.apply("Count values",
				Count.globally())
		.apply("Compare results",
				ParDo.of(new CompareResults(customOptions.getExpectedResult(), customOptions.getLogInfo())));
		p.run();
	}
}