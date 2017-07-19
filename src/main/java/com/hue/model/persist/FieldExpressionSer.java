package com.hue.model.persist;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.hue.model.FieldExpression;

public class FieldExpressionSer extends JsonSerializer<FieldExpression> {

	@Override
	public void serialize(FieldExpression fe, JsonGenerator gen, SerializerProvider serializers) throws IOException {
		gen.writeStartObject();
	    gen.writeStringField("sql", fe.getSql());
	    gen.writeArrayFieldStart("tables");
	    
	    fe.getTables().stream().forEach(t -> { 
	    		String n = t.getDatasource().getName() + "." +
	    			String.join(".", t.getPhysicalNameSegments());
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
