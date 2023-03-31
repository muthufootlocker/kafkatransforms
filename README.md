# Custom Transformation
Custom Transformations (SMTs) applied in source and sink connectors. Before transformation, message need to be in a desired format with respect to predefined transformers.

## Note:
If you're done any modification please update custom-smt.zip file and commit it. because it's used in docker compose file.

## Steps:

### Install Maven
Install the maven to include dependencies in the pom.xml. Then simply run,
```shell
mvn clean install
```
Custom-smt module is there. After building the project the dependency jars and module jar will be created in the
custom-smt-module/target/custom-smt directory.

After that you should compress the custom-smt-module/target/custom-smt folder using the below command

```shell
tar -czvf custom-smt.tar.gz -C custom-smt-module/target custom-smt
```

To unzip this tar file use the below command. just for reference

```shell
tar -xf custom-smt.tar.gz -C .
```

### Docker Build
Check the docker-compose.yml file before proceed the build. There are steps to download the above tar file and unzip and move to connect container




