# Custom Transformation
Custom Transformations (SMTs) applied in source and sink connectors. Before transformation, message need to be in a desired format with respect to predefined transformers.

## Steps
### Install Maven
Install the maven to include dependencies in the pom.xml. Then simply run,
```shell
mvn install
```
custom-date-smt jar and other dependency jar and files will be created in the custom-date-smt/target directory.

you should zip the custom-date-smt folder under the working directory.

### Docker Build
Check the docker-compose.yml file before proceed the build. There are environment variables that need 
to be correct before deployment.

## Note
If you're done any modification please update custom-date-smt.zip file and commit it. because it's used in docker compose file.



