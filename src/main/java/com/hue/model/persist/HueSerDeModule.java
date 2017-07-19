package com.hue.model.persist;

import com.fasterxml.jackson.databind.module.SimpleModule;
import com.hue.model.FieldExpression;
import com.hue.model.Join;

public class HueSerDeModule extends SimpleModule {
	private static final long serialVersionUID = 1L;
	private static final String NAME = "HueSerDeModule";

	  public HueSerDeModule() {
	    super(NAME);
	    addSerializer(FieldExpression.class, new FieldExpressionSer());
	    addSerializer(Join.class, new JoinSer());
	  }
	}