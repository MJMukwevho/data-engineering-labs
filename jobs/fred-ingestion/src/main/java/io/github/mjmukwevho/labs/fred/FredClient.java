package io.github.mjmukwevho.labs.fred;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.util.Optional;

/**
 * Minimal client for the FRED series/observations endpoint.
 * Returns the response body unchanged so the raw layer keeps an exact copy of the source.
 */
public class FredClient {

    public static final String DEFAULT_BASE_URL = "https://api.stlouisfed.org/fred";

    private static final int MAX_ATTEMPTS = 4;

    private final HttpClient http;
    private final String baseUrl;
    private final String apiKey;
    private final Duration initialBackoff;

    public FredClient(String apiKey) {
        this(HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build(),
                DEFAULT_BASE_URL, apiKey, Duration.ofSeconds(2));
    }

    FredClient(HttpClient http, String baseUrl, String apiKey, Duration initialBackoff) {
        this.http = http;
        this.baseUrl = baseUrl;
        this.apiKey = apiKey;
        this.initialBackoff = initialBackoff;
    }

    /** Fetches observations for one series as raw JSON. */
    public String fetchObservations(String seriesId, Optional<LocalDate> observationStart)
            throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(observationsUri(seriesId, observationStart))
                .timeout(Duration.ofSeconds(30))
                .GET()
                .build();

        Duration backoff = initialBackoff;
        for (int attempt = 1; ; attempt++) {
            HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
            int status = response.statusCode();

            if (status == 200) {
                return response.body();
            }
            // 429 (rate limited) and 5xx are transient; anything else (e.g. bad series ID or key) is not.
            boolean retryable = status == 429 || status >= 500;
            if (!retryable || attempt == MAX_ATTEMPTS) {
                throw new IOException("FRED request for " + seriesId + " failed with HTTP " + status
                        + ": " + response.body());
            }
            Thread.sleep(backoff.toMillis());
            backoff = backoff.multipliedBy(2);
        }
    }

    URI observationsUri(String seriesId, Optional<LocalDate> observationStart) {
        StringBuilder url = new StringBuilder(baseUrl)
                .append("/series/observations?series_id=").append(encode(seriesId))
                .append("&api_key=").append(encode(apiKey))
                .append("&file_type=json");
        observationStart.ifPresent(start -> url.append("&observation_start=").append(start));
        return URI.create(url.toString());
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }
}
