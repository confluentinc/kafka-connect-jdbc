package io.confluent.connect.jdbc.gp.gpfdist.framweork;

import java.util.List;

/**
 * Shared boot configuration properties for "spring.net.hostdiscovery".
 *

 * @author Sabby Anandan
 *
 */
public class HostInfoDiscoveryProperties {

	/**
	 * Used to match ip address from a network using a cidr notation
	 */
	private String matchIpv4;

	/**
	 * The new match interface regex pattern. Default value is is empty
	 */
	private String matchInterface;

	/**
	 * The new preferred interface list
	 */
	private List<String> preferInterface;

	/**
	 * The new point to point flag. Default value is FALSE
	 */
	private boolean pointToPoint = false;

	/**
	 * The new loopback flag. Default value is FALSE
	 */
	private boolean loopback = false;

	public String getMatchIpv4() {
		return matchIpv4;
	}

	public void setMatchIpv4(String matchIpv4) {
		this.matchIpv4 = matchIpv4;
	}

	public String getMatchInterface() {
		return matchInterface;
	}

	public void setMatchInterface(String matchInterface) {
		this.matchInterface = matchInterface;
	}

	public List<String> getPreferInterface() {
		return preferInterface;
	}

	public void setPreferInterface(List<String> preferInterface) {
		this.preferInterface = preferInterface;
	}

	public boolean isPointToPoint() {
		return pointToPoint;
	}

	public void setPointToPoint(boolean pointToPoint) {
		this.pointToPoint = pointToPoint;
	}

	public boolean isLoopback() {
		return loopback;
	}

	public void setLoopback(boolean loopback) {
		this.loopback = loopback;
	}
}