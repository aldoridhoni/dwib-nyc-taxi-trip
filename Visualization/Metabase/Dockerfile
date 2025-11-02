FROM eclipse-temurin:21-jre-jammy

# Build arguments for versions
ARG METABASE_VERSION=0.56.9
ARG METABASE_DUCKDB_DRIVER_VERSION=1.4.1.0

ENV MB_PLUGINS_DIR=/app/plugins/

RUN groupadd -r metabase && useradd -r -g metabase metabase

RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# /app/plugins will hold the plugins
# /app/data is not strictly necessary, can be a place to store data files (.duckdb, .parquet, etc)
RUN mkdir -p /app/plugins /app/data /home/metabase && \
    chown -R metabase:metabase /app && \
    chown -R metabase:metabase /home/metabase

WORKDIR /app
ADD --chown=metabase:metabase https://downloads.metabase.com/v${METABASE_VERSION}/metabase.jar /app/
ADD --chown=metabase:metabase https://github.com/motherduckdb/metabase_duckdb_driver/releases/download/${METABASE_DUCKDB_DRIVER_VERSION}/duckdb.metabase-driver.jar /app/plugins/
ADD --chown=metabase:metabase https://raw.githubusercontent.com/metabase/metabase/refs/heads/master/bin/docker/run_metabase.sh /app/

# Ensure proper file permissions
RUN chmod 755 /app/metabase.jar && \
    chmod 755 /app/plugins/duckdb.metabase-driver.jar && \
    chmod +x /app/run_metabase.sh

EXPOSE 3000

USER metabase

# CMD ["java", "-jar", "/app/metabase.jar"
ENTRYPOINT ["/app/run_metabase.sh"]