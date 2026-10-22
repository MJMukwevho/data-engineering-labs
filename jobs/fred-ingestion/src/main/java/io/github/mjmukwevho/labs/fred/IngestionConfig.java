package io.github.mjmukwevho.labs.fred;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Job settings, read from environment variables.
 *
 * <ul>
 *   <li>{@code FRED_API_KEY} (required): free key from https://fred.stlouisfed.org/docs/api/api_key.html</li>
 *   <li>{@code FRED_SERIES}: comma-separated series IDs, defaults to {@link #DEFAULT_SERIES}</li>
 *   <li>{@code FRED_OBSERVATION_START}: ISO date (yyyy-MM-dd); omit to load full history</li>
 *   <li>{@code RAW_DATA_LOCATION}: root of the raw layer, either {@code s3://bucket/prefix} or a
 *       local folder; defaults to {@code ./data/raw}. {@code RAW_DATA_DIR} is accepted as an older name.</li>
 *   <li>{@code S3_FORCE_PATH_STYLE}: {@code true} for local S3-compatible stores; leave unset on AWS</li>
 *   <li>{@code FRED_BASE_URL}: override the FRED API address (for testing)</li>
 * </ul>
 */
public record IngestionConfig(
        String apiKey,
        List<String> seriesIds,
        Optional<LocalDate> observationStart,
        String rawLocation,
        boolean s3ForcePathStyle,
        String fredBaseUrl) {

    /** S&P 500, Dow Jones, NASDAQ Composite, VIX and the 10-year Treasury yield. */
    public static final List<String> DEFAULT_SERIES =
            List.of("SP500", "DJIA", "NASDAQCOM", "VIXCLS", "DGS10");

    public static IngestionConfig fromEnvironment(Map<String, String> env) {
        String apiKey = env.get("FRED_API_KEY");
        if (apiKey == null || apiKey.isBlank()) {
            throw new IllegalArgumentException(
                    "FRED_API_KEY is not set. Get a free key at https://fred.stlouisfed.org/docs/api/api_key.html");
        }

        List<String> seriesIds = nonBlank(env, "FRED_SERIES")
                .map(s -> Arrays.stream(s.split(","))
                        .map(String::trim)
                        .filter(id -> !id.isEmpty())
                        .toList())
                .orElse(DEFAULT_SERIES);

        Optional<LocalDate> observationStart = nonBlank(env, "FRED_OBSERVATION_START").map(LocalDate::parse);

        String rawLocation = nonBlank(env, "RAW_DATA_LOCATION")
                .or(() -> nonBlank(env, "RAW_DATA_DIR"))
                .orElse("data/raw");

        boolean s3ForcePathStyle = nonBlank(env, "S3_FORCE_PATH_STYLE").map(Boolean::parseBoolean).orElse(false);

        String fredBaseUrl = nonBlank(env, "FRED_BASE_URL").orElse(FredClient.DEFAULT_BASE_URL);

        return new IngestionConfig(apiKey.trim(), seriesIds, observationStart, rawLocation, s3ForcePathStyle, fredBaseUrl);
    }

    private static Optional<String> nonBlank(Map<String, String> env, String name) {
        return Optional.ofNullable(env.get(name)).map(String::trim).filter(s -> !s.isEmpty());
    }
}
