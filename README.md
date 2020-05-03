# cadastre-web-service project

## Running the application in dev mode

You can run your application in dev mode that enables live coding using:
```
./mvnw quarkus:dev
```

## Packaging and running the application

The application can be packaged using `./mvnw package`.
It produces the `cadastre-web-service-0.0.1-SNAPSHOT-runner.jar` file in the `/target` directory.
Be aware that it’s not an _über-jar_ as the dependencies are copied into the `target/lib` directory.

The application is now runnable using `java -jar target/cadastre-web-service-0.0.1-SNAPSHOT-runner.jar`.

## Creating a native executable

You can create a native executable using: `./mvnw package -Pnative`.

Or, if you don't have GraalVM installed, you can run the native executable build in a container using: `./mvnw package -Pnative -Dquarkus.native.container-build=true`.

You can then execute your native executable with: `./target/cadastre-web-service-0.0.1-SNAPSHOT-runner`

If you want to learn more about building native executables, please consult https://quarkus.io/guides/building-native-image.

## Database

```
docker run --rm --name grundstuecksinformation-db -p 54321:5432 --hostname primary -e PG_DATABASE=grundstuecksinformation -e PG_LOCALE=de_CH.UTF-8 -e PG_PRIMARY_PORT=5432 -e PG_MODE=primary -e PG_USER=admin -e PG_PASSWORD=admin -e PG_PRIMARY_USER=repl -e PG_PRIMARY_PASSWORD=repl -e PG_ROOT_PASSWORD=secret -e PG_WRITE_USER=gretl -e PG_WRITE_PASSWORD=gretl -e PG_READ_USER=ogc_server -e PG_READ_PASSWORD=ogc_server -v ~/pgdata-grundstuecksinformation:/pgdata:delegated sogis/oereb-db:latest
```

## Test requests
```
curl -X GET -H "Accept: application/xml" -H "Content-Type: application/xml" http://localhost:8080/getegrid/xml/?XY=2600564,1215478 > response.xml && xmllint --format response.xml
curl -X GET -H "Accept: application/xml" -H "Content-Type: application/xml" http://localhost:8080/getegrid/xml/?XY=2600466,1215406 > response.xml && xmllint --format response.xml
curl -X GET -H "Accept: application/xml" -H "Content-Type: application/xml" http://localhost:8080/getegrid/xml/SO0200002457/452/ > response.xml && xmllint --format response.xml
curl -X GET -H "Accept: application/xml" -H "Content-Type: application/xml" http://localhost:8080/extract/xml/geometry/CH955832730623 > response.xml && xmllint --format response.xml

```