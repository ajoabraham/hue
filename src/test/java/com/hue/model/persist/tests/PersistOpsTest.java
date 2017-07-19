package com.hue.model.persist.tests;

import static org.junit.Assert.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.hue.model.Project;
import com.hue.model.persist.HueSerDeModule;
import org.apache.commons.io.IOUtils;

public class PersistOpsTest {
	private static ObjectMapper mapper;
	
	@Before
	public void setup(){
		mapper = new ObjectMapper();
        mapper.registerModule(new HueSerDeModule());
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
	}
	
	@Test
	public void testSerialization() throws IOException {
		Project pj = new Project();
		pj.setName("Test Project");
		String pjs = mapper.writeValueAsString(pj);
		
		Path resourceDirectory = Paths.get("projects/test_project");
		 String pjt = IOUtils.toString(
			      new FileInputStream(resourceDirectory.toString()+"/project.json"),
			      "UTF-8"
			    );
		
		assertEquals(pjs, pjt);
	}

}
