package com.hue.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Measure extends Expressible {
	private static final long serialVersionUID = 1L;
	
	public Measure() {}
	
}
