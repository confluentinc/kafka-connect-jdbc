package io.confluent.connect.jdbc.gp.gpfdist.framweork.support;

public interface GreenplumLoad {

	public void load() throws Exception;

	public void load(RuntimeContext context) throws Exception;

}
