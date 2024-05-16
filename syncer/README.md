* get into the container
`docker compose exec syncer /bin/bash`

* start the process
`java --add-opens=java.base/java.nio=ALL-UNNAMED -jar target/syncer-1.0-SNAPSHOT.jar`

* it should try to dump stuff in /output which is mounted as a volume

