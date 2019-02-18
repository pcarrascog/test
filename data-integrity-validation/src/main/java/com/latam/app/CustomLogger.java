package com.latam.app;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.json.JSONObject;

import com.google.cloud.MonitoredResource;
import com.google.cloud.logging.LogEntry;
import com.google.cloud.logging.Logging;
import com.google.cloud.logging.LoggingOptions;
import com.google.cloud.logging.Payload.JsonPayload;
import com.google.cloud.logging.Severity;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CustomLogger {
	
	private static final Logger LOG = LoggerFactory.getLogger(CustomLogger.class);

	private static LoggingOptions options = LoggingOptions.getDefaultInstance();
	
	public static void WriteLog(String logInfo, String message, String sender, Severity severity) {
		JSONObject jsonLogInfo = new JSONObject(logInfo);
		Logging logging = options.getService();
		Map<String, Object> jsonMap = new HashMap<>();
		Iterator<String> keys = jsonLogInfo.keys();

		while(keys.hasNext()) {
		    String key = keys.next();
	    	jsonMap.put(key.toLowerCase(), jsonLogInfo.getString(key));
		}
		
		jsonMap.put("message", message);
		jsonMap.put("sender", "DATAFLOW - " + sender);
		LogEntry entry = LogEntry.newBuilder(JsonPayload.of(jsonMap))
		        .setSeverity(severity)
		        .setLogName(jsonLogInfo.getString("LOG_NAME"))
		        .setResource(MonitoredResource.newBuilder("global").build())
		        .build();
		logging.write(Collections.singleton(entry));		
	}

}
