package io.github.mjmukwevho.labs.fred;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Runs the client against a local stub server, so no network access or API key is needed. */
class FredClientTest {

    private HttpServer server;
    private final Deque<Integer> statuses = new ArrayDeque<>();
    private final AtomicInteger requests = new AtomicInteger();
    private final AtomicReference<String> lastQuery = new AtomicReference<>();
    private FredClient client;

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/fred/series/observations", exchange -> {
            requests.incrementAndGet();
            lastQuery.set(exchange.getRequestURI().getRawQuery());
            int status = statuses.isEmpty() ? 200 : statuses.poll();
            byte[] body = (status == 200 ? "{\"observations\":[]}" : "{\"error_code\":" + status + "}")
                    .getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();

        String baseUrl = "http://127.0.0.1:" + server.getAddress().getPort() + "/fred";
        HttpClient http = HttpClient.newBuilder().proxy(HttpClient.Builder.NO_PROXY).build();
        client = new FredClient(http, baseUrl, "test-key", Duration.ofMillis(1));
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
    }

    @Test
    void returnsBodyUnchangedOnSuccess() throws Exception {
        String body = client.fetchObservations("SP500", Optional.empty());

        assertEquals("{\"observations\":[]}", body);
        assertEquals(1, requests.get());
    }

    @Test
    void sendsSeriesKeyFormatAndStartDate() throws Exception {
        client.fetchObservations("SP500", Optional.of(LocalDate.of(2024, 1, 31)));

        String query = lastQuery.get();
        assertTrue(query.contains("series_id=SP500"), query);
        assertTrue(query.contains("api_key=test-key"), query);
        assertTrue(query.contains("file_type=json"), query);
        assertTrue(query.contains("observation_start=2024-01-31"), query);
    }

    @Test
    void retriesRateLimitAndServerErrors() throws Exception {
        statuses.add(429);
        statuses.add(503);

        client.fetchObservations("SP500", Optional.empty());

        assertEquals(3, requests.get());
    }

    @Test
    void doesNotRetryClientErrors() {
        statuses.add(400);

        IOException e = assertThrows(IOException.class,
                () -> client.fetchObservations("NOT_A_SERIES", Optional.empty()));

        assertTrue(e.getMessage().contains("HTTP 400"), e.getMessage());
        assertEquals(1, requests.get());
    }

    @Test
    void givesUpAfterMaxAttempts() {
        for (int i = 0; i < 10; i++) {
            statuses.add(500);
        }

        assertThrows(IOException.class, () -> client.fetchObservations("SP500", Optional.empty()));
        assertEquals(4, requests.get());
    }
}
