package com.hue.model.persist;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.hue.model.IJoinable;
import com.hue.model.Join;

public class JoinSer extends JsonSerializer<Join> {

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
