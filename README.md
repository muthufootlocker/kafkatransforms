# Custom Transformation
Custom Transformations (SMTs) applied in source and sink connectors. Before transformation, message need to be in a desired format with respect to predefined transformers.

## Steps:

### Install Maven
Install the maven to include dependencies in the pom.xml. Then simply run,
```shell
mvn clean install
```
Custom-smt module is there. After building the project the dependency jars and module jar will be created in the
custom-smt-module/custom-smt directory.

After that you can commit your changes.

```shell
You should create the release tag and use it in the docker compose file for every new release.
```

### Docker Build
Check the docker-compose.yml file before proceed the build. There are steps to download the above tar file and unzip and move to connect container




