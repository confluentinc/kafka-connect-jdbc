package io.confluent.connect.jdbc.gp.gpfdist.framweork.support;

public interface LoadService {

	public void load(LoadConfiguration loadConfiguration) throws Exception;

	public void load(LoadConfiguration loadConfiguration, RuntimeContext context) throws Exception;

}
