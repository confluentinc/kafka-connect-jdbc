
## Readme2 :)

.: Some useful mvn targets for locally building, testing & packaging the connector

### Clean `./target` directory
```shell
% mvn clean 
```

### Compile 
```shell
% mvn clean compile 
```

### Tests
Run all JUnit tests
```shell
% mvn test 
```


### Selected Tests
Run only tests in the specified class, e.g `RedshiftDatabaseDialectTest`
```shell
% mvn test -Dtest=RedshiftDatabaseDialectTest
```

Run only specified test methods in the class e.g `shouldBuildUpsertStatement`
```shell
% mvn test -Dtest=RedshiftDatabaseDialectTest#shouldBuildUpsertStatement
```


### Package (Generate Jar artifact)

To create an artifact (JAR) locally in the /target directory, use the following.

> *NOTE: This target will run all tests and thus my take longer to finish; See next section to skip tests during packaging*

```shell
% mvn clean package 
```

To skip all tests, can take long if you want to simply create a JAR to use.
```shell
% mvn clean package -DskipTests
```


