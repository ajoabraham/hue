package com.hue.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Dimension extends Expressible {
	private static final long serialVersionUID = 1L;
	
	public Dimension() {}
	
}
