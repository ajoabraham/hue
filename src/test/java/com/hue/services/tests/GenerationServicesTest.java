package com.hue.services.tests;

import java.io.IOException;
import java.util.Optional;

import org.junit.Test;

import com.hue.graph.GraphException;
import com.hue.graph.GraphOps;
import com.hue.model.persist.ProjectServices;
import com.hue.model.persist.Schema;
import com.hue.services.GenerationServices;
import com.hue.services.ServiceException;

public class GenerationServicesTest {

	@Test
	public void test() throws IOException, ServiceException, GraphException {
		ProjectServices ps = ProjectServices.getInstance();
		ps.loadProjects();
		
		Optional<Schema> s = ps.getSchema("Shop");
		s.get().buildGraph();
		System.out.println(GraphOps.qeGetBasePaths(s.get(), s.get().getDimension("Category ID").get(), 
				s.get().getMeasure("Sales").get()));
//		GenerationServices.genSchema(s.get(), "lodr_dev_local", true, true, true, false,true);
	}
	
	@Test
	public void testGenShopDb() throws ServiceException, IOException {
		ProjectServices ps = ProjectServices.getInstance();
		ps.loadProjects();
		GenerationServices.genSchema(ps.getSchema("Shop").get(), "shop_db", true, true, true, false,true);
	}
}
