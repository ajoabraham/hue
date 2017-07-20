package com.hue.model.persist;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.hue.common.CardinalityType;
import com.hue.common.JoinType;
import com.hue.model.Datasource;
import com.hue.model.IJoinable;
import com.hue.model.Join;
import com.hue.model.Table;

public class JoinSerDe{
	private JoinSerDe() {};
	
	public static class JoinSer extends JsonSerializer<Join> {

		@Override
		public void serialize(Join join, JsonGenerator gen, SerializerProvider serializers) throws IOException {
			gen.writeStartObject();
		    
			gen.writeStringField("sql", join.getSql());
		    gen.writeStringField("leftTable", tableName(join.getLeft()));
		    gen.writeStringField("rightTable", tableName(join.getRight()));
		    gen.writeNumberField("cost", join.getCost());
		    gen.writeNumberField("allowRollDown", join.getCost());
		    gen.writeStringField("cardinalityType", join.getCardinalityType().toString());
		    gen.writeStringField("joinType", join.getJoinType().toString());
			
		    gen.writeEndObject();		
		}
		
		private String tableName(IJoinable t) {
			return String.join(".", t.getPhysicalNameSegments());
		}
	}
	
	public static class JoinDeSer extends JsonDeserializer<Join>{

		@Override
		public Join deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
			Schema s = (Schema) ctxt.findInjectableValue("schema", null, null);
			Datasource ds = (Datasource) ctxt.findInjectableValue("datasource", null, null);
			
			ObjectCodec oc = p.getCodec();
		    JsonNode node = oc.readTree(p);
			
		    check(node,"leftTableName");		    
		    final String left = node.get("leftTableName").asText();
		    String[] l = left.split(".");
		    Table leftTbl;
		    if(l.length > 1) {
		    		leftTbl = s.getTable(ds, l[l.length-1], l[l.length-2]).get();
		    }else {
		    		leftTbl = s.getTable(ds, left).get();
		    }
		    
		    check(node,"rightTableName");
		    final String right = node.get("rightTableName").asText();
		    String[] r = right.split(".");
		    Table rightTbl;
		    if(r.length > 1) {
		    		rightTbl = s.getTable(ds, r[r.length-1], r[r.length-2]).get();
		    }else {
		    		rightTbl = s.getTable(ds, right).get();
		    }
		    
		    check(node,"sql");
		    Join j = new Join(
		    		leftTbl,
		    		rightTbl,
		    		node.get("sql").asText(),
		    		null
		    	);
		    
		    check(node,"joinType");
		    JoinType jt = JoinType.valueOf(node.get("joinType").asText("INNER_JOIN"));
		    check(node,"cardinalityType");
		    CardinalityType ct = CardinalityType.valueOf(node.get("cardinalityType").asText());
		    j.setJoinType(jt);
		    j.setCardinalityType(ct);
		    
		    int ar = -1;
		    if(node.get("allowRollDown") != null) {
		    		ar = node.get("allowRollDown").asInt(-1);	
		    }
		    
		    if(ar == 1) {
		    		j.setAllowRollDown(true);
		    }else if(ar == 0) {
		    		j.setAllowRollDown(false);
		    }		    
		    
			return j;
		}
		
		private void check(JsonNode node, String field) throws IOException {
			if(node.get(field) == null) {
    				throw new IOException("required property " +field + " not found");
			}
	    }
		
	}
}

//{
//	  "sql" : "schema_name.table_name.col1 = schema_name2.table_name2.col1_t2",
//	  "leftTableName" : "schema_name.table_name",
//	  "rightTableName" : "schema_name2.table_name2",
//	  "cost" : 1000,
//	  "allowRollDown" : -1,
//	  "cardinalityType" : "ONE_TO_MANY",
//	  "joinType" : "INNER_JOIN"
//	}

