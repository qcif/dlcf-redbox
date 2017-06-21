package com.googlecode.fascinator.redbox.plugins.curation.dlcf;

import com.googlecode.fascinator.common.transaction.GenericTransactionManager;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.fascinator.api.PluginException;
import com.googlecode.fascinator.api.PluginManager;
import com.googlecode.fascinator.api.indexer.Indexer;
import com.googlecode.fascinator.api.storage.DigitalObject;
import com.googlecode.fascinator.api.storage.Payload;
import com.googlecode.fascinator.api.storage.Storage;
import com.googlecode.fascinator.api.storage.StorageException;
import com.googlecode.fascinator.api.transaction.TransactionException;
import com.googlecode.fascinator.common.JsonObject;
import com.googlecode.fascinator.common.JsonSimple;
import com.googlecode.fascinator.common.JsonSimpleConfig;
import com.googlecode.fascinator.common.transaction.GenericTransactionManager;
import com.googlecode.fascinator.messaging.TransactionManagerQueueConsumer;

public class DLCFTransactionManager extends GenericTransactionManager {
	/** Logging **/
	private static Logger log = LoggerFactory.getLogger(DLCFTransactionManager.class);


	/** System configuration */
	private JsonSimpleConfig systemConfig;

	/** Storage */
	private Storage storage;

	/** Solr Index */
	private Indexer indexer;

	/** External URL base */
	private String urlBase;

	/**
	 * Base constructor
	 *
	 */
	public DLCFTransactionManager() {
		super("dlcf", "DLCF Curation Transaction Manager");
	}

	/**
	 * Initialise method
	 *
	 * @throws TransactionException
	 *             if there was an error during initialisation
	 */
	@Override
	public void init() throws TransactionException {
		systemConfig = getJsonConfig();

		// Load the storage plugin
		String storageId = systemConfig.getString("file-system", "storage", "type");
		if (storageId == null) {
			throw new TransactionException("No Storage ID provided");
		}
		storage = PluginManager.getStorage(storageId);
		if (storage == null) {
			throw new TransactionException("Unable to load Storage '" + storageId + "'");
		}
		try {
			storage.init(systemConfig.toString());
		} catch (PluginException ex) {
			log.error("Unable to initialise storage layer!", ex);
			throw new TransactionException(ex);
		}

		// Load the indexer plugin
		String indexerId = systemConfig.getString("solr", "indexer", "type");
		if (indexerId == null) {
			throw new TransactionException("No Indexer ID provided");
		}
		indexer = PluginManager.getIndexer(indexerId);
		if (indexer == null) {
			throw new TransactionException("Unable to load Indexer '" + indexerId + "'");
		}
		try {
			indexer.init(systemConfig.toString());
		} catch (PluginException ex) {
			log.error("Unable to initialise indexer!", ex);
			throw new TransactionException(ex);
		}

		// External facing URL
		urlBase = systemConfig.getString(null, "urlBase");
		if (urlBase == null) {
			throw new TransactionException("URL Base in config cannot be null");
		}
	}

	/**
	 * Shutdown method
	 *
	 * @throws PluginException
	 *             if any errors occur
	 */
	@Override
	public void shutdown() throws PluginException {
		if (storage != null) {
			try {
				storage.shutdown();
			} catch (PluginException pe) {
				log.error("Failed to shutdown storage: {}", pe.getMessage());
				throw pe;
			}
		}
		if (indexer != null) {
			try {
				indexer.shutdown();
			} catch (PluginException pe) {
				log.error("Failed to shutdown indexer: {}", pe.getMessage());
				throw pe;
			}
		}
	}

	/**
	 * Processing method
	 *
	 * @param message
	 *            The JsonSimple message to process
	 * @return JsonSimple The actions to take in response
	 * @throws TransactionException
	 *             If an error occurred
	 */
	@Override
	public JsonSimple parseMessage(JsonSimple message) throws TransactionException {
		log.debug("\n{}", message.toString(true));

		// A standard harvest event
		JsonObject harvester = message.getObject("harvester");
		String repoType = message.getString("", "indexer", "params", "repository.type");
		String oid = message.getString(null, "oid");
		if (harvester != null && !"Attachment".equalsIgnoreCase(repoType)) {
			try {

				JsonSimple response = new JsonSimple();
				audit(response, oid, "Tool Chain");

				// Standard transformers... ie. not related to curation
				scheduleTransformers(message, response);

				// Solr Index
				JsonObject order = newIndex(response, oid);
				order.put("forceCommit", true);

				// Send a message back here
				createTask(response, oid, "clear-render-flag");
				return response;
			} catch (Exception ex) {
				throw new TransactionException(ex);
			}
		} else {
			log.debug("Is type attachment, ignoring...");
		}

		// It's not a harvest, what else could be asked for?
		String task = message.getString(null, "task");

		// ######################
		// Start a reharvest for this object
		if (task.equals("reharvest")) {
			JsonSimple response = new JsonSimple();
			reharvest(response, message);
			return response;
		}

		// ######################
		// Tool chain, clear render flag
		if (task.equals("clear-render-flag")) {
			if (oid != null) {
				clearRenderFlag(oid);
			} else {
				log.error("Cannot clear render flag without an OID!");
			}
		}

		// Do nothing
		return new JsonSimple();

	}

	/**
	 * Generate a fairly common list of orders to transform and index an object.
	 * This mirrors the traditional tool chain.
	 *
	 * @param message
	 *            The response to modify
	 * @param message
	 *            The message we received
	 */
	private void reharvest(JsonSimple response, JsonSimple message) {
		String oid = message.getString(null, "oid");

		try {
			if (oid != null) {
				setRenderFlag(oid);

				// Transformer config
				JsonSimple itemConfig = getConfigFromStorage(oid);
				if (itemConfig == null) {
					log.error("Error accessing item configuration!");
					return;
				}
				itemConfig.getJsonObject().put("oid", oid);

				// Tool chain
				scheduleTransformers(itemConfig, response);
				JsonObject order = newIndex(response, oid);
				order.put("forceCommit", true);
				createTask(response, oid, "clear-render-flag");
			} else {
				log.error("Cannot reharvest without an OID!");
			}
		} catch (Exception ex) {
			log.error("Error during reharvest setup: ", ex);
		}
	}

	/**
	 * Generate an order to add a message to the System's audit log
	 *
	 * @param response
	 *            The response to add an order to
	 * @param oid
	 *            The object ID we are logging
	 * @param message
	 *            The message we want to log
	 */
	private void audit(JsonSimple response, String oid, String message) {
		JsonObject order = newSubscription(response, oid);
		JsonObject messageObject = (JsonObject) order.get("message");
		messageObject.put("eventType", message);
	}

	/**
	 * Generate orders for the list of normal transformers scheduled to execute
	 * on the tool chain
	 *
	 * @param message
	 *            The incoming message, which contains the tool chain config for
	 *            this object
	 * @param response
	 *            The response to edit
	 * @param oid
	 *            The object to schedule for clearing
	 */
	private void scheduleTransformers(JsonSimple message, JsonSimple response) {
		String oid = message.getString(null, "oid");
		List<String> list = message.getStringList("transformer", "metadata");
		if (list != null && !list.isEmpty()) {
			for (String id : list) {
				JsonObject order = newTransform(response, id, oid);
				// Add item config to message... if it exists
				JsonObject itemConfig = message.getObject("transformerOverrides", id);
				if (itemConfig != null) {
					JsonObject config = (JsonObject) order.get("config");
					config.putAll(itemConfig);
				}
			}
		}
	}

	/**
	 * Creation of new Orders with appropriate default nodes
	 *
	 */
	private JsonObject newIndex(JsonSimple response, String oid) {
		JsonObject order = createNewOrder(response, TransactionManagerQueueConsumer.OrderType.INDEXER.toString());
		order.put("oid", oid);
		return order;
	}

	/**
	 * Create a task. Tasks are basically just trivial messages that will come
	 * back to this manager for later action.
	 *
	 * @param response
	 *            The response to edit
	 * @param oid
	 *            The object to schedule for clearing
	 * @param task
	 *            The task String to use on receipt
	 * @return JsonObject Access to the 'message' node of this task to provide
	 *         further details after creation.
	 */
	private JsonObject createTask(JsonSimple response, String oid, String task) {
		return createTask(response, null, oid, task);
	}

	/**
	 * Create a task. This is a more detailed option allowing for tasks being
	 * sent to remote brokers.
	 *
	 * @param response
	 *            The response to edit
	 * @param broker
	 *            The broker URL to use
	 * @param oid
	 *            The object to schedule for clearing
	 * @param task
	 *            The task String to use on receipt
	 * @return JsonObject Access to the 'message' node of this task to provide
	 *         further details after creation.
	 */
	private JsonObject createTask(JsonSimple response, String broker, String oid, String task) {
		JsonObject object = newMessage(response, TransactionManagerQueueConsumer.LISTENER_ID);
		if (broker != null) {
			object.put("broker", broker);
		}
		JsonObject message = (JsonObject) object.get("message");
		message.put("task", task);
		message.put("oid", oid);
		return message;
	}

	/**
	 * Clear the render flag for objects that have finished in the tool chain
	 *
	 * @param oid
	 *            The object to clear
	 */
	private void clearRenderFlag(String oid) {
		try {
			DigitalObject object = storage.getObject(oid);
			Properties props = object.getMetadata();
			props.setProperty("render-pending", "false");
			object.close();
		} catch (StorageException ex) {
			log.error("Error accessing storage for '{}'", oid, ex);
		}
	}

	/**
	 * Set the render flag for objects that are starting in the tool chain
	 *
	 * @param oid
	 *            The object to set
	 */
	private void setRenderFlag(String oid) {
		try {
			DigitalObject object = storage.getObject(oid);
			Properties props = object.getMetadata();
			props.setProperty("render-pending", "true");
			object.close();
		} catch (StorageException ex) {
			log.error("Error accessing storage for '{}'", oid, ex);
		}
	}

	/**
	 * Get the stored harvest configuration from storage for the indicated
	 * object.
	 *
	 * @param oid
	 *            The object we want config for
	 */
	private JsonSimple getConfigFromStorage(String oid) {
		String configOid = null;
		String configPid = null;

		// Get our object and look for its config info
		try {
			DigitalObject object = storage.getObject(oid);
			Properties metadata = object.getMetadata();
			configOid = metadata.getProperty("jsonConfigOid");
			configPid = metadata.getProperty("jsonConfigPid");
		} catch (StorageException ex) {
			log.error("Error accessing object '{}' in storage: ", oid, ex);
			return null;
		}

		// Validate
		if (configOid == null || configPid == null) {
			log.error("Unable to find configuration for OID '{}'", oid);
			return null;
		}

		// Grab the config from storage
		try {
			DigitalObject object = storage.getObject(configOid);
			Payload payload = object.getPayload(configPid);
			try {
				return new JsonSimple(payload.open());
			} catch (IOException ex) {
				log.error("Error accessing config '{}' in storage: ", configOid, ex);
			} finally {
				payload.close();
			}
		} catch (StorageException ex) {
			log.error("Error accessing object in storage: ", ex);
		}

		// Something screwed the pooch
		return null;
	}

	private JsonObject newSubscription(JsonSimple response, String oid) {
		JsonObject order = createNewOrder(response, TransactionManagerQueueConsumer.OrderType.SUBSCRIBER.toString());
		order.put("oid", oid);
		JsonObject message = new JsonObject();
		message.put("oid", oid);
		message.put("context", "Curation");
		message.put("eventType", "Sending test message");
		message.put("user", "system");
		order.put("message", message);
		return order;
	}

	private JsonObject createNewOrder(JsonSimple response, String type) {
		JsonObject order = response.writeObject("orders", -1);
		order.put("type", type);
		return order;
	}

	private JsonObject newTransform(JsonSimple response, String target, String oid) {
		JsonObject order = createNewOrder(response, TransactionManagerQueueConsumer.OrderType.TRANSFORMER.toString());
		order.put("target", target);
		order.put("oid", oid);
		JsonObject config = systemConfig.getObject("transformerDefaults", target);
		if (config == null) {
			order.put("config", new JsonObject());
		} else {
			order.put("config", config);
		}

		return order;
	}

	private JsonObject newMessage(JsonSimple response, String target) {
		JsonObject order = createNewOrder(response,
		TransactionManagerQueueConsumer.OrderType.MESSAGE.toString());
		order.put("target", target);
		order.put("message", new JsonObject());
		return order;
	}
}
