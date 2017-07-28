package com.hue.model.persist;

import java.io.IOException;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.hue.model.Datasource;
import com.hue.model.FieldExpression;
import com.hue.model.Table;
import com.hue.utils.CommonUtils;

public class FieldExpSerDe{
	private FieldExpSerDe() {};
	private static final Logger logger = LoggerFactory.getLogger(FieldExpSerDe.class.getName());
	
	public static class FieldExpSer extends JsonSerializer<FieldExpression> {

		@Override
		public void serialize(FieldExpression fe, JsonGenerator gen, SerializerProvider serializers) throws IOException {
			gen.writeStartObject();
		    gen.writeStringField("sql", fe.getSql());
		    gen.writeArrayFieldStart("tables");
		    
		    fe.getTables().stream().forEach(t -> { 
		    		String n = t.getDatasource().getName() + ".";
		    		if(!CommonUtils.isBlank(t.getSchemaName())) n = n + t.getSchemaName() + ".";
		    		
		    		n = n + t.getName();
		    		
	    			try {
					gen.writeString(n);
				} catch (IOException e) {
					new RuntimeException("Could not write out expression tables for " + fe.getSql() + ". \n" + e.getMessage());
				}
		    });
		    
		    gen.writeEndArray();
		    gen.writeEndObject();		
		}

	}
	
	public static class FieldExpDeSer extends JsonDeserializer<FieldExpression>{

		@Override
		public FieldExpression deserialize(JsonParser p, DeserializationContext ctxt)
				throws IOException, JsonProcessingException {
			Schema s = (Schema) ctxt.findInjectableValue("schema", null, null);
			
			ObjectCodec oc = p.getCodec();
		    JsonNode node = oc.readTree(p);
		    
		    check(node,"sql");
		    FieldExpression fe = new FieldExpression();
		    fe.setSql(node.get("sql").asText());
		    
		    check(node,"tables");
		    if(node.get("tables").isArray()) {
		    		for(final JsonNode obj : node.get("tables")) {
		    			String[] t = obj.asText().split("\\.");
		    			if(t.length>1) {
		    				String ds = t[0];
		    				Optional<Datasource> dsI = s.getDatasource(ds);
	    					if(!dsI.isPresent()) {
	    						throw new IOException("could not find datasource " + ds + " for project " + s.getProject().getName());
	    					}
	    					
		    				String tableName = t[t.length-1];
		    				if(t.length>2) {
		    					String schemaN = t[t.length-2];
		    					Optional<Table> tt = s.getTable(dsI.get(), tableName, schemaN);
		    					if(!tt.isPresent()) {
		    						throw new IOException("could not find table " + schemaN + "." + tableName + " for datasource " + ds);
		    					}
		    					fe.getTables().add(tt.get());
		    				}else {
		    					Optional<Table> tt = s.getTable(dsI.get(), tableName);
		    					if(!tt.isPresent()) {
		    						throw new IOException("could not find table " + "." + tableName + " for datasource " + ds);
		    					}
		    					fe.getTables().add(tt.get());
		    				}
		    			}else {
		    				logger.error("tables should be defined as datasource_name.table_name or datasource_name.schema_name.table_name (spaces in names are okay)");
		    			}
		    		}
		    }else {
		    		throw new IOException("tables property should be an array.");
		    }
		    
			return fe;
		}
		
		private void check(JsonNode node, String field) throws IOException {
			if(node.get(field) == null) {
    				throw new IOException("required property " +field + " not found");
			}
	    }
		
	}
}

//{
//    "sql" : "first_name || last_name",
//    "tables" : [ "example ds.schema_name2.table_name2" ]
//  }

