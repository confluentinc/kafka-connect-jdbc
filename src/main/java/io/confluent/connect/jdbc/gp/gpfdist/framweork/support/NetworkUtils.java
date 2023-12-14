package io.confluent.connect.jdbc.gp.gpfdist.framweork.support;

/**
 * Various network utilities.
 *

 *
 */
public class NetworkUtils {

	public static String getGPFDistUri(String address, int port) {
		return "gpfdist://" + address + ":" + port + "/data";
	}
}
