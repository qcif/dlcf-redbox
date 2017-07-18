import com.google.gson.JsonPrimitive


/*
 * The Fascinator - Plugin - Transformer - Raid Curation
 * Copyright (C) 2017 Queensland Cyber Infrastructure Foundation (http://www.qcif.edu.au/)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */
package com.googlecode.fascinator.redbox.plugins.raid;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import com.googlecode.fascinator.api.PluginDescription;
import com.googlecode.fascinator.api.PluginException;
import com.googlecode.fascinator.api.PluginManager;
import com.googlecode.fascinator.api.storage.DigitalObject;
import com.googlecode.fascinator.api.storage.Payload;
import com.googlecode.fascinator.api.storage.Storage;
import com.googlecode.fascinator.api.storage.StorageException;
import com.googlecode.fascinator.api.transformer.Transformer;
import com.googlecode.fascinator.api.transformer.TransformerException;
import com.googlecode.fascinator.common.JsonObject;
import com.googlecode.fascinator.common.JsonSimple;
import com.googlecode.fascinator.common.JsonSimpleConfig;
import com.googlecode.fascinator.common.storage.StorageUtils;
import com.jayway.jsonpath.Configuration;
@GrabResolver(name='redbox', root='http://dev.redboxresearchdata.com.au/nexus/content/groups/public/')
@Grab(group='au.com.redboxresearchdata', module='json-path', version='2.4.0')
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.Configuration.Defaults;
import com.jayway.jsonpath.internal.DefaultsImpl;
import com.jayway.jsonpath.spi.json.GsonJsonProvider;
import com.squareup.okhttp.FormEncodingBuilder;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.RequestBody;
import com.squareup.okhttp.Response;
import org.apache.commons.lang.StringUtils;

/**
 * <p>
 *  Transformer that will fetch a RAID via the RAID rest api
 * </p>
 *
 *
 * @author Andrew Brazzatti
 */
public class RaidTransformer implements Transformer {

	/** Logging **/
	private static Logger log = LoggerFactory.getLogger(RaidTransformer.class);

	/** Configuration */
	private JsonSimpleConfig config;

	/** Storage layer */
	private Storage storage;

	/** Flag for first execution */
	private boolean firstExecution;

	/** Curation PID Property */
	private String pidProperty;

	OkHttpClient client;

	private String targetDatastream;

	private String triggerCondition;

	private String triggerDatastream;

	private Configuration jsonPathConfiguration;

	private String apiUrl;

	private String apiKey;

	/**
	 * Constructor
	 */
	public RaidTransformer() {
		firstExecution = true;
		// Initialise JsonPath classes for speed
		Defaults defaults = DefaultsImpl.INSTANCE;
		jsonPathConfiguration = Configuration.builder().jsonProvider(new GsonJsonProvider()).options(defaults.options())
				.build();
		JsonPath.isPathDefinite('$');
		client = new OkHttpClient();
	}

	/**
	 * Init method from file
	 *
	 * @param jsonFile
	 * @throws IOException
	 * @throws PluginException
	 */
	@Override
	public void init(File jsonFile) throws PluginException {
		try {
			config = new JsonSimpleConfig(jsonFile);
			reset();
		} catch (IOException e) {
			throw new PluginException("Error reading config: ", e);
		}
	}

	/**
	 * Init method from String
	 *
	 * @param jsonString
	 * @throws IOException
	 * @throws PluginException
	 */
	@Override
	public void init(String jsonString) throws PluginException {
		try {
			config = new JsonSimpleConfig(jsonString);
			reset();
		} catch (IOException e) {
			throw new PluginException("Error reading config: ", e);
		}
	}

	/**
	 * Reset the transformer in preparation for a new object
	 */
	private void reset() throws TransformerException {
		if (firstExecution) {
			if (storage == null) {
				try {
					String storageType = config.getString(null, "storage", "type");
					storage = PluginManager.getStorage(storageType);
					storage.init(JsonSimpleConfig.getSystemFile());
				} catch (Exception ex) {
					throw new TransformerException(ex);
				}
			}

			// Where are we storing our finished PIDs
			pidProperty = config.getString(null, "transformerDefaults", "raid", "raidProperty", "property");
			if (pidProperty == null || "".equals(pidProperty)) {
				throw new TransformerException("No (or invalid) PID property" + " found in config");
			}

			targetDatastream = config.getString(".tfpackage", "transformerDefaults", "raid", "raidProperty",
					"datastream");

			triggerCondition = config.getString(null, "transformerDefaults", "raid", "trigger", "condition");

			triggerDatastream = config.getString(".tfpackage", "transformerDefaults", "raid", "trigger", "datastream");

			apiUrl = config.getString("https://api.raid.org.au:8000/", "raid", "api", "url");

			apiKey = config.getString(null, "raid", "api", "key");

			// Make sure we don't end up here again
			firstExecution = false;
		}

	}

	/**
	 * Transform method
	 *
	 * @param object
	 *            : DigitalObject to be transformed
	 * @param jsonConfig
	 *            : String containing configuration for this item
	 * @return DigitalObject The object after being transformed
	 * @throws TransformerException
	 *             gh
	 */
	// public DigitalObject transform(DigitalObject in, String jsonConfig)
	// throws TransformerException {
	public DigitalObject transform(DigitalObject digitalObject, String jsonConfig) throws TransformerException {
		// Read item config and reset before we start
		JsonSimpleConfig itemConfig = null;
		try {
			itemConfig = new JsonSimpleConfig(jsonConfig);
		} catch (IOException ex) {
			throw new TransformerException("Error reading item configuration!", ex);
		}
		reset();

		try {
			if (triggerCondition != null) {
				JsonSimple triggerDatastreamSimple = getJsonSimpleForDatastream(triggerDatastream, digitalObject);

				DocumentContext triggerStreamContext = JsonPath
						.parse(new JsonParser().parse(triggerDatastreamSimple.toString()), jsonPathConfiguration);
				
				 JsonArray response = triggerStreamContext.read('$.[?(@.' +triggerCondition + ')]');
				if (response.size() == 0) {
					// condition failed
					return digitalObject;
				}

			}

			JsonSimple targetDatastreamSimple = getJsonSimpleForDatastream(targetDatastream, digitalObject);
			DocumentContext targetStreamContext = JsonPath
					.parse(new JsonParser().parse(targetDatastreamSimple.toString()), jsonPathConfiguration);
	
			def currentRaid = targetStreamContext.read('$.'+pidProperty);
			log.info("Check value exists response: " + currentRaid.getClass().getName())
			log.info("Check value exists response: " + (currentRaid instanceof com.google.gson.JsonObject))
			def needsRaid = false;
			//If the raid property was there it would be a String
			if (targetStreamContext.read('$.'+pidProperty) instanceof com.google.gson.JsonObject ) {
				needsRaid = true;
			}
			
			if(targetStreamContext.read('$.'+pidProperty) instanceof com.google.gson.JsonPrimitive && StringUtils.isBlank(((com.google.gson.JsonPrimitive)context.read("$.metadata.raid")).getAsString())){
				needsRaid = true;
			}
			
			if(needsRaid) {
				String raid = getRaid();
				if(raid != null) {
				targetStreamContext.configuration().addOptions(Option.WRITE_IF_KEY_NOT_EXIST);
				
					targetStreamContext.set('$.' + pidProperty, getRaid());
				}
			}

			String updatedJson = targetStreamContext.jsonString();

			saveUpdatedJson(new JsonSimple(updatedJson).getJsonObject(),
					getPidForExtension(targetDatastream, digitalObject), digitalObject);

		} catch (StorageException e) {
			throw new TransformerException(e);
		} catch (IOException e2) {
			throw new TransformerException(e2);
		}

		return digitalObject;
	}

	private String getRaid() throws StorageException {
		
	// TODO: add config for start date selector and add it to the request
//		RequestBody formBody = new FormEncodingBuilder().build();
		// .add("start_date", new
		// DateTime().toString(ISODateTimeFormat.dateTime())).build();

		Request request = new Request.Builder().url(apiUrl+"/raids/").addHeader("Content-Type", "application/json")
				.addHeader("Authorization", "ApiKey " + apiKey).post().build();

		Response response;
		try {
			response = client.newCall(request).execute();

			if (response.code() != 201) {
				throw new StorageException("API call failed: \n" + response.body().string());
			}

			return new JsonSimple(response.body().string()).getString(null, "raid_id");
		} catch (IOException e) {
			throw new StorageException(e);
		}
	}

	private void saveUpdatedJson(JsonObject jsonObject, String pid, DigitalObject digitalObject)
			throws StorageException {
		if (pid.equals("TF-OBJ-META")) {
			Properties metadataProperties = digitalObject.getMetadata();
			metadataProperties.clear();
			Set<Object> keys = jsonObject.keySet();
			for (Object key : keys) {
				metadataProperties.setProperty((String) key, (String) jsonObject.get(key));
			}
			digitalObject.close();
		} else {
			String jsonString = new JsonSimple(jsonObject).toString(true);
			try {
				InputStream inStream = new ByteArrayInputStream(jsonString.getBytes("UTF-8"));
				StorageUtils.createOrUpdatePayload(digitalObject, pid, inStream);
			} catch (UnsupportedEncodingException e) {
				throw new StorageException(e);
			}

		}
	}

	private JsonSimple getJsonSimpleForDatastream(String datastream, DigitalObject digitalObject)
			throws StorageException, IOException {
		String pid = getDatastreamPid(datastream, digitalObject);

		if ("TF-OBJ-META".equals(pid)) {
			return getObjectPropertiesJson(digitalObject.getMetadata());
		}

		return getJsonPayload(digitalObject.getPayload(pid));
	}

	private String getDatastreamPid(String datastream, DigitalObject digitalObject) {
		if (datastream.startsWith(".")) {
			return getPidForExtension(datastream, digitalObject);
		}

		return datastream;
	}

	private String getPidForExtension(String datastreamSuffix, DigitalObject digitalObject) {
		Set<String> payloads = digitalObject.getPayloadIdList();

		for (String payload : payloads) {
			if (payload.endsWith(datastreamSuffix)) {
				return payload;
			}
		}

		return null;
	}

	private JsonSimple getJsonPayload(Payload payload) throws StorageException, IOException {
		return new JsonSimple(payload.open());
	}

	private JsonSimple getObjectPropertiesJson(Properties metadata) {
		JsonObject object = new JsonObject();
		Set<String> propertyKeys = metadata.stringPropertyNames();
		for (String key : propertyKeys) {
			object.put(key, metadata.getProperty(key));
		}
		return new JsonSimple(object);
	}

	/**
	 * Get Transformer ID
	 *
	 * @return id
	 */
	@Override
	public String getId() {
		return "raid";
	}

	/**
	 * Get Transformer Name
	 *
	 * @return name
	 */
	@Override
	public String getName() {
		return "Raid Curation Transformer";
	}

	/**
	 * Gets a PluginDescription object relating to this plugin.
	 *
	 * @return a PluginDescription
	 */
	@Override
	public PluginDescription getPluginDetails() {
		return new PluginDescription(this);
	}

	/**
	 * Shut down the transformer plugin
	 */
	@Override
	public void shutdown() throws PluginException {
		if (storage != null) {
			storage.shutdown();
		}
	}
}