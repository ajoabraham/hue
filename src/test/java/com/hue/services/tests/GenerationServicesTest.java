package com.hue.services.tests;

import java.io.IOException;
import java.util.Optional;

import org.junit.Test;

import com.hue.model.persist.ProjectServices;
import com.hue.model.persist.Schema;
import com.hue.services.GenerationServices;
import com.hue.services.ServiceException;

public class GenerationServicesTest {

	@Test
	public void test() throws IOException, ServiceException {
		ProjectServices ps = ProjectServices.getInstance();
		ps.loadProjects();
		
		Optional<Schema> s = ps.getSchema("lodr_dev");
		s.get().buildGraphs();
//		GenerationServices.genSchema(s.get(), "lodr_dev_local", true, true, true, false,true);
	}

}
