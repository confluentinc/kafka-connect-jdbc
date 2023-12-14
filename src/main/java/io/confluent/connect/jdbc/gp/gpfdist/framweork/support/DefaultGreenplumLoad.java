package io.confluent.connect.jdbc.gp.gpfdist.framweork.support;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultGreenplumLoad implements GreenplumLoad {

	private static final Logger log = LoggerFactory.getLogger(DefaultGreenplumLoad.class);

	private final LoadService loadService;

	private final LoadConfiguration loadConfiguration;

	public DefaultGreenplumLoad(LoadConfiguration loadConfiguration, LoadService loadService) {
		this.loadConfiguration = loadConfiguration;
		this.loadService = loadService;

//		Assert.notNull(loadConfiguration, "Load configuration must be set");
//		Assert.notNull(loadService, "Load service must be set");
	}

	@Override
	public void load() throws Exception {
		load(null);
	}

	@Override
	public void load(RuntimeContext context) throws Exception {
		log.debug("Doing greenplum load");
		loadService.load(loadConfiguration, context);
	}

}
