Telemetry Pipeline
    consists of major components
    - producer
    - consumer
    - api-server

Producer:
- Periodically Reads the data from csv file
- Batches the records/messages
- Sends/Produce it to Message Queue

Consumer:
- Fetches messages from message queue
- Process/Enrich the GPU telemetry
- inserts records to DB (Postgres/TimescaleDB)

API Server:
- Expose the HTTP api server on port 8080
- Connect to DB and Queries data
- Process the records and return response.

For local testing:
    Run Postgres DB locally
    ```
        docker run --name pg -e POSTGRES_PASSWORD=pass -e POSTGRES_USER=user -e POSTGRES_DB=telemetry -p 5432:5432 -d postgres:15
    ```
To connect K8s postgres DB

PostgreSQL can be accessed via port 5432 on the following DNS names from within your cluster:

    timescaledb-postgresql.telemetry.svc.cluster.local - Read/Write connection

To get the password for "postgres" run:

    export POSTGRES_ADMIN_PASSWORD=$(kubectl get secret --namespace telemetry timescaledb-postgresql -o jsonpath="{.data.postgres-password}" | base64 -d)

To get the password for "telemetry" run:

    export POSTGRES_PASSWORD=$(kubectl get secret --namespace telemetry timescaledb-postgresql -o jsonpath="{.data.password}" | base64 -d)

To connect to your database run the following command:

    kubectl run timescaledb-postgresql-client --rm --tty -i --restart='Never' --namespace telemetry --image docker.io/bitnami/postgresql:17.6.0-debian-12-r0 --env="PGPASSWORD=$POSTGRES_PASSWORD" \
      --command -- psql --host timescaledb-postgresql -U telemetry -d telemetry -p 5432

    > NOTE: If you access the container using bash, make sure that you execute "/opt/bitnami/scripts/postgresql/entrypoint.sh /bin/bash" in order to avoid the error "psql: local user with ID 1001} does not exist"

To connect to your database from outside the cluster execute the following commands:

    kubectl port-forward --namespace telemetry svc/timescaledb-postgresql 5432:5432 &
    PGPASSWORD="$POSTGRES_PASSWORD" psql --host 127.0.0.1 -U telemetry -d telemetry -p 5432

WARNING: The configured password will be ignored on new installation in case when previous PostgreSQL release was deleted through the helm command. In that case, old PVC will have an old password, and setting it through helm won't take effect. Deleting persistent volumes (PVs) will solve the issue.

WARNING: There are "resources" sections in the chart not set. Using "resourcesPreset" is not recommended for production. For production installations, please set the following values according to your workload needs:
  - primary.resources
  - readReplicas.resources
+info https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/