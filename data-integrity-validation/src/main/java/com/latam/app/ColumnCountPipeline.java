package com.latam.app;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.cloud.MonitoredResource;
import com.google.cloud.logging.LogEntry;
import com.google.cloud.logging.Logging;
import com.google.cloud.logging.LoggingOptions;
import com.google.cloud.logging.Severity;
import com.google.cloud.logging.Payload.JsonPayload;
import java.util.Collections;
import org.json.JSONObject;
import java.util.HashMap;
import java.util.Map;


public class ColumnCountPipeline {

	private static final Logger LOG = LoggerFactory.getLogger(ColumnCountPipeline.class);
	private static LoggingOptions options = LoggingOptions.getDefaultInstance();

	public interface CustomOptions extends PipelineOptions {

		@Description("GCS location of input file")
		@Required
		ValueProvider<String> getInputFilepattern();
		void setInputFilepattern(ValueProvider<String> value);

		@Description("Expected column count")
		@Required
		ValueProvider<Integer> getExpectedColumns();
		void setExpectedColumns(ValueProvider<Integer> value);

		@Description("Delimiter")
		@Required
		ValueProvider<String> getDelimiter();
		void setDelimiter(ValueProvider<String> value);

		@Description("Log info")
		@Required
		ValueProvider<String> getLogInfo();
		void setLogInfo(ValueProvider<String> value);

	}


	@SuppressWarnings("Annotator")
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

	static class CompareColumns extends DoFn<ArrayList<String>, Integer> {
        ValueProvider<Integer> expected;

        CompareColumns(ValueProvider<Integer> expectedColumns) {
            this.expected = expectedColumns;
        }

		@ProcessElement
		public void processElement(@Element ArrayList<String> line, OutputReceiver<Integer> out) {
			if(Integer.parseInt(expected.get().toString()) == line.size())
			    out.output(0);
            else
                out.output(1);
		}
	}

static class CompareResults extends DoFn<Integer, Boolean> {
		ValueProvider<Integer> expectedColumns;
		ValueProvider<String> logInfo;

		CompareResults(ValueProvider<Integer> expectedColumns, ValueProvider<String> logInfo) {
			this.expectedColumns = expectedColumns;
			this.logInfo = logInfo;
			

		}

		@ProcessElement
		public void processElement(@Element Integer result, OutputReceiver<Boolean> out) {
			
			if(result == 0) {
				CustomLogger.WriteLog(logInfo.get(),
						"El numero de columnas es valido para todos los registros. Resultado: " + expectedColumns.get().toString(),
						"ColumnCountPipeline",
						Severity.INFO);				
			}else {	
				LOG.info("El numero de columnas no coincide con el valor esperado de " + expectedColumns.get().toString());
				CustomLogger.WriteLog(logInfo.get(),
						"El numero de columnas no coincide con el valor esperado de " + expectedColumns.get().toString(),
						"ColumnCountPipeline",
						Severity.ERROR);

				throw new java.lang.Error("El numero de columnas no coincide con el valor esperado de " + expectedColumns.get().toString());
			}
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException {
		PipelineOptionsFactory.register(CustomOptions.class);
		CustomOptions customOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(CustomOptions.class);
		customOptions.setJobName("columncountvalidation" + new Date().getTime());

		Pipeline p = Pipeline.create(customOptions);
		p.apply("Read File",
				TextIO.read()
						.from(customOptions.getInputFilepattern()))
				.apply("Split file lines",
						ParDo.of(new SplitLines(customOptions.getDelimiter())))
				.apply("Count Columns",
						ParDo.of(new CompareColumns(customOptions.getExpectedColumns())))
				.apply("Sum Results",
						Sum.integersGlobally())
				.apply("Compare results",
						ParDo.of(new CompareResults(customOptions.getExpectedColumns(), customOptions.getLogInfo())));
		p.run();
	}
}