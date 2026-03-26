# Stage 1: Builder
FROM eclipse-temurin:17-jdk AS builder
WORKDIR /build

COPY build.xml VERSION ./
COPY src/ src/
COPY lib/ lib/
COPY ext/ ext/
COPY thirdparty/ thirdparty/
COPY base-server/ base-server/
COPY base-client/ base-client/
COPY base-monitor-client/ base-monitor-client/
COPY docs/ docs/
COPY scripts/ scripts/

RUN sed -i 's/\r$//' ext/ant/bin/ant && chmod +x ext/ant/bin/ant && ext/ant/bin/ant package \
    && mkdir -p /build/client-extracted && cd /build/client-extracted && jar xf /build/build/package/slamd/slamd-client-*.zip


# Stage 2: SLAMD Server
FROM eclipse-temurin:17-jre AS server
WORKDIR /opt/slamd

COPY --from=builder /build/build/package/slamd/ .
COPY infra/docker/slamd_metrics.properties webapps/slamd/WEB-INF/slamd_metrics.properties
COPY infra/docker/server-entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh && chmod +x bin/*.sh

EXPOSE 8080 3000 3001 3003
VOLUME ["/opt/slamd/webapps/slamd/WEB-INF/db"]
ENTRYPOINT ["/entrypoint.sh"]


# Stage 3: SLAMD Client
FROM eclipse-temurin:17-jre AS client
WORKDIR /opt/slamd-client

COPY --from=builder /build/client-extracted/slamd-client/ .
RUN chmod +x /opt/slamd-client/*.sh && chmod +x /opt/slamd-client/tools/*.sh

COPY infra/docker/slamd-client.conf slamd-client.conf
COPY infra/docker/client-entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
