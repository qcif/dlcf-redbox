import java.util.Date
import java.util.HashMap
import org.apache.solr.common.SolrInputDocument
import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.ISODateTimeFormat
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.github.wnameless.json.flattener.FlattenMode
import com.github.wnameless.json.flattener.JsonFlattener;
import com.googlecode.fascinator.common.JsonSimple
import org.apache.commons.lang.StringUtils;
import org.json.simple.JSONArray;
import com.googlecode.fascinator.common.JsonObject;


SolrInputDocument document = new SolrInputDocument();
def oid = object.getId();
document.setField("id",oid);
document.addField("storage_id",object.getId());

if(payload.getId().endsWith(".tfpackage")){
	JsonSimple fullJson = new JsonSimple(payload.open());

	for(def topLevelKey in fullJson.getJsonObject().keySet()) {
		def topLevelValue = fullJson.getJsonObject().get(topLevelKey);
		if(topLevelValue instanceof JsonObject) {
			def prefix = topLevelKey+"_";
			if(topLevelKey.equals("metadata")) {
				prefix = "";
			}
			indexObject(document, topLevelValue, prefix);
		} else {
			document.addField(topLevelKey,topLevelValue);
		}
	}


}

def pid = payload.getId()
def metadataPid = params.getProperty("metaPid", "DC")
def itemType= "object"
if(pid != metadataPid) {
	itemType = "datastream"
	document.setField("id",object.getId()+"/"+pid);
	document.addField("identifier",pid)
}

document.addField("item_type",itemType);
document.addField("last_modified",new Date().format("YYYY-MM-DD'T'hh:mm:ss'Z'"));
document.addField("harvest_config",params.getProperty("jsonConfigOid"));
document.addField("harvest_rules",params.getProperty("rulesOid"));

document.addField("owner",params.getProperty("owner", "guest"))

DateTimeFormatter dateFormatter = ISODateTimeFormat.dateTime()
def dateObjectCreated = params.getProperty("date_object_created")
def dateObjectModified = params.getProperty("date_object_modified")
document.addField("date_object_created", dateFormatter.parseDateTime(dateObjectCreated).toDate())
if(!StringUtils.isBlank(dateObjectModified)) {
	document.addField("date_object_modified", dateFormatter.parseDateTime(dateObjectModified).toDate())
} else {
	document.addField("date_object_modified", dateFormatter.parseDateTime(dateObjectCreated).toDate())
}

return document;


def Date parseDate(String value) {
	Date date = parseDate(value,ISODateTimeFormat.date());

	if(date == null) {
		date = parseDate(value,ISODateTimeFormat.dateTime());
	}

	if(date == null) {
		date = parseDate(value,ISODateTimeFormat.dateTimeNoMillis());
	}

	return date;
}

def Date parseDate(String value, DateTimeFormatter dateFormatter) {
	Date date = null;
	try {
		date = dateFormatter.parseDateTime(value).toDate();
	}catch(IllegalArgumentException e) {
		//not a date
	}
	return date;
}

def void indexObject(def document, JsonObject jsonObject, String prefix) {
	String flattenedMetadataJson = new JsonFlattener(new JsonSimple(jsonObject).toString()).withFlattenMode(FlattenMode.MONGODB).flatten();

	ObjectMapper mapper = new ObjectMapper();
	TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {};

	HashMap<String, Object> tfPackageMap = mapper.readValue(flattenedMetadataJson, typeRef);



	for (key in tfPackageMap.keySet()) {
		def value = tfPackageMap.get(key);
		def fieldKey = prefix+key;
		if(value instanceof String) {
			//may be a date string
			Date date = parseDate((String) value);
			if (date != null) {
				// It's a date so add the value as in a date specific solr field (starting with date_)
				document.addField("date_"+fieldKey,date);
			}


			if(fieldKey.matches(".*\\.[0-9]+")){
				document.addField(fieldKey.substring(0, fieldKey.lastIndexOf('.')),tfPackageMap.get(key));
			} else {
				document.addField(fieldKey,tfPackageMap.get(key));
			}
		}

		if(value instanceof Integer) {
			document.addField("int_"+fieldKey,tfPackageMap.get(key));
			document.addField(fieldKey,tfPackageMap.get(key).toString());
		}

		//TODO: Float/Doubles don't appear to be picked up properly by the Jackson Parser
		if(value instanceof Double) {
			document.addField("float_"+fieldKey, (float)tfPackageMap.get(key).doubleValue());
			document.addField(fieldKey,tfPackageMap.get(key).toString());
		}

		if(value instanceof Float) {
			document.addField("float_"+fieldKey, tfPackageMap.get(key));
			document.addField(fieldKey,tfPackageMap.get(key).toString());
		}

		if(value instanceof Boolean) {
			document.addField("bool_"+fieldKey,tfPackageMap.get(key));
			document.addField(fieldKey,tfPackageMap.get(key).toString());
		}

	}
}

