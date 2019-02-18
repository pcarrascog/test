package com.latam.app;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.MonitoredResource;
import com.google.cloud.logging.LogEntry;
import com.google.cloud.logging.Logging;
import com.google.cloud.logging.LoggingOptions;
import com.google.cloud.logging.Severity;
import com.google.cloud.logging.Payload.JsonPayload;

import com.latam.app.SumBigDecimalFn;

public class FieldSummationPipeline {

	private static final Logger LOG = LoggerFactory.getLogger(FieldSummationPipeline.class);
	private static LoggingOptions options = LoggingOptions.getDefaultInstance();
	
	public interface CustomOptions extends PipelineOptions {

		@Description("GCS location of input file")
		@Required
		ValueProvider<String> getInputFilepattern();
		void setInputFilepattern(ValueProvider<String> value);

		@Description("Position of the column to sum")
		@Required
		ValueProvider<Integer> getColumnPosition();
		void setColumnPosition(ValueProvider<Integer> value);		
		
		@Description("Expected result of the sum")
		@Required
		ValueProvider<Double> getExpectedResult();
		void setExpectedResult(ValueProvider<Double> value);
		
		@Description("Delimiter")
		@Required
		ValueProvider<String> getDelimiter();
		void setDelimiter(ValueProvider<String> value);
		
		@Description("StackDriver log information")
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
	
	static class TrimFields extends DoFn<ArrayList<String>, ArrayList<String>> {
		@ProcessElement
		public void processElement(@Element ArrayList<String> line, OutputReceiver<ArrayList<String>> out) {
			
			for(int i = 0; i < line.size(); i++) {
				line.set(i, line.get(i).trim());
			}
			out.output(line);
			
		}
	}
	
	static class FilterField extends DoFn<ArrayList<String>, BigDecimal> {
		ValueProvider<Integer> columnIndex;
		ValueProvider<String> logInfo;
		
		FilterField(ValueProvider<Integer> columnPosition, ValueProvider<String> logInfo) {
			this.columnIndex = columnPosition;
			this.logInfo = logInfo;
		}
		
		@ProcessElement
		public void processElement(@Element ArrayList<String> line, OutputReceiver<BigDecimal> out) {
			BigDecimal result;
			try {
				result = new BigDecimal(line.get(columnIndex.get()));
				out.output(result);
			} catch (IndexOutOfBoundsException | NumberFormatException e) {
				CustomLogger.WriteLog(logInfo.get(), 
						"field_summation_validation - El valor de la columna " + columnIndex.get() + 
						" no pudo ser convertido al tipo de dato BigDecimal. " + e.toString(),
						"field_summation_validation",
						Severity.ERROR);
				throw new java.lang.Error("El valor de la columna " + columnIndex.get()  + 
						" no pudo ser convertido al tipo de dato BigDecimal. " + e.toString());
			}
		}
	}
	
	static class CompareResults extends DoFn<BigDecimal, Boolean> {
		ValueProvider<Double> expected;
		ValueProvider<String> logInfo;
		
		CompareResults(ValueProvider<Double> expectedResult, ValueProvider<String> logInfo) {
			this.expected = expectedResult;
			this.logInfo = logInfo;
			}
		
		@ProcessElement
		public void processElement(@Element BigDecimal result, OutputReceiver<Boolean> out) {
			BigDecimal ecxaxtResult = new BigDecimal(expected.get().toString());
			if(ecxaxtResult.compareTo(result) == 0) {
				LOG.info("field_summation_validation - El resultado de la suma es valido. Resultado: " + result.toString() + " / Esperado: " + ecxaxtResult.toString());
				CustomLogger.WriteLog(logInfo.get(), 
						"field_summation_validation - El resultado de la suma es valido. Resultado: " + result.toString() + " / Esperado: " + ecxaxtResult.toString(),
						"field_summation_validation",
						Severity.INFO);
			}else {
				LOG.info("field_summation_validation - El resultado de la suma " + result.toString() + " no coincide con el valor esperado " + ecxaxtResult.toString());
				CustomLogger.WriteLog(logInfo.get(), 
						"field_summation_validation - El resultado de la suma " + result.toString() + " no coincide con el valor esperado " + ecxaxtResult.toString(),
						"field_summation_validation",
						Severity.ERROR);
				throw new java.lang.Error("field_summation_validation - El resultado de la suma " + result.toString() + " no coincide con el valor esperado " + ecxaxtResult.toString());
					}
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException {
		PipelineOptionsFactory.register(CustomOptions.class);
		CustomOptions customOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(CustomOptions.class);
		customOptions.setJobName("fieldsumvalidation" + new Date().getTime());

		Pipeline p = Pipeline.create(customOptions);
		p.apply("Read File", 
				TextIO.read()
				.from(customOptions.getInputFilepattern()))
		.apply("Split file lines",
				ParDo.of(new SplitLines(customOptions.getDelimiter())))
		.apply("Trim values",
						ParDo.of(new TrimFields()))
		.apply("Filter field",
					ParDo.of(new FilterField(customOptions.getColumnPosition(), customOptions.getLogInfo())))
		.apply("Summation of values",
				Combine.globally(new SumBigDecimalFn()))
		.apply("Compare results",
				ParDo.of(new CompareResults(customOptions.getExpectedResult(), customOptions.getLogInfo())));
		p.run();
	}
}
