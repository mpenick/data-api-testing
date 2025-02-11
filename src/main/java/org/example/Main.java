package org.example;

import ch.qos.logback.classic.Level;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeStateListener;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.session.ProgrammaticArguments;
import com.datastax.oss.driver.api.core.session.SessionBuilder;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.channel.ChannelFactory;
import com.datastax.oss.driver.internal.core.channel.ConnectInitHandler;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.channel.DriverChannelOptions;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.context.DefaultNettyOptions;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.context.NettyOptions;
import com.datastax.oss.driver.internal.core.util.concurrent.PromiseCombiner;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.datastax.oss.protocol.internal.util.collection.NullAllowingImmutableMap;
import com.sun.net.httpserver.HttpServer;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.netty.channel.*;
import io.netty.handler.codec.haproxy.*;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class Main {

    public static class TenantAwareCqlSessionBuilder extends CqlSessionBuilder {
        private final String tenantId;

        public TenantAwareCqlSessionBuilder(String tenantId) {
            if (tenantId == null || tenantId.isEmpty()) {
                throw new IllegalArgumentException("tenantId cannot be null or empty");
            }
            this.tenantId = tenantId;
        }

        @Override
        protected DriverContext buildContext(
                DriverConfigLoader configLoader, ProgrammaticArguments programmaticArguments) {
            return new TenantAwareDriverContext(tenantId, configLoader, programmaticArguments);
        }
    }

    public static class TenantAwareDriverContext extends DefaultDriverContext {
        private final String tenantId;

        public TenantAwareDriverContext(String tenantId, DriverConfigLoader configLoader, ProgrammaticArguments programmaticArguments) {
            super(configLoader, programmaticArguments);
            this.tenantId = tenantId;
        }

        @Override
        protected Map<String, String> buildStartupOptions() {
            Map<String, String> existing = super.buildStartupOptions();
            return NullAllowingImmutableMap.<String, String>builder(existing.size() + 1)
                    .putAll(existing).put("TENANT_ID", tenantId)
                    .build();
        }
    }

    public static void printRow(Row row) {
        System.out.println(row.getFormattedContents());
    }

    public static void main(String[] args) throws Exception {
        java.security.Security.setProperty("networkaddress.cache.ttl", "-1");

        String token = null, tenant = null, host = null, localDc = "europe-west4";
        int port = 29099;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--host":
                    if (i + 1 >= args.length) {
                        throw new IllegalArgumentException("Must provide a host");
                    }
                    host = args[++i];
                    break;
                case "--tenant":
                    if (i + 1 >= args.length) {
                        throw new IllegalArgumentException("Must provide a tenant (database ID)");
                    }
                    tenant = args[++i];
                    break;
                case "--token":
                    if (i + 1 >= args.length) {
                        throw new IllegalArgumentException("Must provide a token");
                    }
                    token = args[++i];
                    break;
                case "--log-level":
                    if (i + 1 >= args.length) {
                        throw new IllegalArgumentException("Must provide a log level");
                    }
                    Logger logger = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
                    if (logger instanceof ch.qos.logback.classic.Logger) {
                        Level level = ch.qos.logback.classic.Level.valueOf(args[++i]);
                        ((ch.qos.logback.classic.Logger)logger).setLevel(level);
                    }
                    break;
                case "--dc":
                case "--local-dc": // Fallthrough
                    if (i + 1 >= args.length) {
                        throw new IllegalArgumentException("Must provide a DC");
                    }
                    localDc = args[++i];
                    break;
                case "port":
                    if (i + 1 >= args.length) {
                        throw new IllegalArgumentException("Must provide a port");
                    }
                    port = Integer.parseInt(args[++i]);
                    break;
            }
        }

        if (host == null || host.isEmpty()) {
            throw new IllegalArgumentException("Invalid host");
        }

        if (token == null || token.isEmpty()) {
            throw new IllegalArgumentException("Invalid token");
        }

        if (tenant == null || tenant.isEmpty()) {
            throw new IllegalArgumentException("Invalid tenant (database ID)");
        }

        if (localDc == null || localDc.isEmpty()) {
            throw new IllegalArgumentException("Invalid local DC");
        }

        try (CqlSession session = new TenantAwareCqlSessionBuilder(tenant)
                .withConfigLoader(DriverConfigLoader
                        .programmaticBuilder()
                        .withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofMillis(10000))
                        .withBoolean(DefaultDriverOption.METADATA_TOKEN_MAP_ENABLED, false)
                        .build())
                .addContactPoint(new InetSocketAddress(host, port))
                .withNodeStateListener(new NodeStateListener() {
                    @Override
                    public void onAdd(@NonNull Node node) {
                        System.out.printf("Adding node %s\n", node);
                    }

                    @Override
                    public void onUp(@NonNull Node node) {
                        System.out.printf("Node %s is up\n", node);
                    }

                    @Override
                    public void onDown(@NonNull Node node) {
                        System.out.printf("Node %s is down\n", node);
                    }

                    @Override
                    public void onRemove(@NonNull Node node) {
                        System.out.printf("Removing node %s\n", node);
                    }

                    @Override
                    public void close() throws Exception {
                    }
                })
                .withLocalDatacenter(localDc)
                .withAuthCredentials("token", token)
                .build()) {
            System.out.println();
            ResultSet rsLocal = session.execute("select * from system.local");
            System.out.println("system.local:");
            printRow(rsLocal.one());
            ResultSet rsPeers = session.execute(SimpleStatement.newInstance("select * from system.peers").setNode(rsLocal.getExecutionInfo().getCoordinator()));
            List<Row> rows = rsPeers.all();
            System.out.printf("system.peers (rows: %d):\n", rows.size());
            for (Row row : rows) {
                printRow(row);
            }
            System.out.println();

            System.out.println("Waiting for events...");
            while (true) {
                try {
                    session.execute("select * from test.test");
                } catch (Exception e) {
                    System.err.println(e);
                } finally {
                    Thread.sleep(500);
                }
            }
        }
    }
}
