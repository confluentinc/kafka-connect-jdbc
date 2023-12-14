package io.confluent.connect.jdbc.gp.gpfdist.framweork.support;

import java.util.ArrayList;
import java.util.List;

/**
 * Runtime context for load operations.
 *

 */
public class RuntimeContext {

	private final List<String> locations;

	/**
	 * Instantiates a new runtime context.
	 */
	public RuntimeContext() {
		this.locations = new ArrayList<>();
	}

	/**
	 * Instantiates a new runtime context.
	 *
	 * @param location the location
	 */
	public RuntimeContext(String location) {
		this.locations = new ArrayList<>();
		addLocation(location);
	}

	/**
	 * Gets the locations.
	 *
	 * @return the locations
	 */
	public List<String> getLocations() {
		return locations;
	}

	/**
	 * Sets the locations.
	 *
	 * @param locations the new locations
	 */
	public void setLocations(List<String> locations) {
		this.locations.clear();
		this.locations.addAll(locations);
	}

	/**
	 * Adds the location.
	 *
	 * @param location the location
	 */
	public void addLocation(String location) {
		this.locations.add(location);
	}
}
