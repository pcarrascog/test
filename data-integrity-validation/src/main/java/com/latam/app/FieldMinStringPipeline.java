package com.latam.app;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.Min;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.DoFn.Element;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;

public class FieldMinStringPipeline {

	private static final Logger LOG = LoggerFactory.getLogger(FieldMinStringPipeline.class);
	
	public interface CustomOptions extends PipelineOptions {

		@Description("GCS location of input file")
		@Required
		ValueProvider<String> getInputFilepattern();
		void setInputFilepattern(ValueProvider<String> value);

		@Description("Position of the column to min")
		@Required
		ValueProvider<Integer> getColumnPosition();
		void setColumnPosition(ValueProvider<Integer> value);
		
		@Description("Expected min length")
		@Required
		ValueProvider<Integer> getExpectedResult();
		void setExpectedResult(ValueProvider<Integer> value);
		
		@Description("Delimiter")
		@Required
		ValueProvider<String> getDelimiter();
		void setDelimiter(ValueProvider<String> value);

	}
	
	static class SplitLines extends DoFn<String, ArrayList<String>> {
		ValueProvider<String> delimiter;
		Boolean flag = null;

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
	
	
	static class FilterField extends DoFn<ArrayList<String>, Integer> {
		ValueProvider<Integer> columnIndex;
		
		FilterField(ValueProvider<Integer> columnPosition) {
			this.columnIndex = columnPosition;
		}
		
		@ProcessElement
		public void processElement(@Element ArrayList<String> line, OutputReceiver<Integer> out) {
			out.output(Integer.valueOf(line.get(columnIndex.get()).length()));
		}
	}
	
	static class CompareResults extends DoFn<Integer, Boolean> {
		ValueProvider<Integer> expected;
		
		CompareResults(ValueProvider<Integer> expectedResult) {
			this.expected = expectedResult;
		}
		
		@ProcessElement
		public void processElement(@Element Integer result, OutputReceiver<Boolean> out) {
			
			if(expected.get().toString().equals(result.toString())) {
				LOG.info("The Min String is a valid. Result: " + result.toString() + " / Expected: " + expected.get().toString());
				out.output(true);
			}else {
				LOG.info("The Min String " + result.toString() + " does not match the expected value of " + expected.get().toString());
				throw new java.lang.Error("The row count " + result.toString() + " does not match the expected value of " + expected.get().toString());
			}
		}
	}

	
	public static void main(String[] args) throws IOException, ClassNotFoundException {
		PipelineOptionsFactory.register(CustomOptions.class);
		CustomOptions customOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(CustomOptions.class);
		customOptions.setJobName("fieldminstringpipeline" + new Date().getTime());

		Pipeline p = Pipeline.create(customOptions);
		p.apply("Read File", 
				TextIO.read()
				.from(customOptions.getInputFilepattern()))
		.apply("Split file lines",
				ParDo.of(new SplitLines(customOptions.getDelimiter())))
		.apply("Filter field and get lengths",
				ParDo.of(new FilterField(customOptions.getColumnPosition())))
		.apply("Min Length", 
				Min.integersGlobally())
		.apply("Compare results",
				ParDo.of(new CompareResults(customOptions.getExpectedResult())));
		p.run();
	}
}