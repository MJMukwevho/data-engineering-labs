package io.github.mjmukwevho.labs.fred;

import java.nio.file.Path;
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
 *   <li>{@code RAW_DATA_DIR}: root of the raw layer, defaults to {@code ./data/raw}</li>
 * </ul>
 */
public record IngestionConfig(
        String apiKey,
        List<String> seriesIds,
        Optional<LocalDate> observationStart,
        Path rawDataDir) {

    /** S&P 500, Dow Jones, NASDAQ Composite, VIX and the 10-year Treasury yield. */
    public static final List<String> DEFAULT_SERIES =
            List.of("SP500", "DJIA", "NASDAQCOM", "VIXCLS", "DGS10");

    public static IngestionConfig fromEnvironment(Map<String, String> env) {
        String apiKey = env.get("FRED_API_KEY");
        if (apiKey == null || apiKey.isBlank()) {
            throw new IllegalArgumentException(
                    "FRED_API_KEY is not set. Get a free key at https://fred.stlouisfed.org/docs/api/api_key.html");
        }

        List<String> seriesIds = Optional.ofNullable(env.get("FRED_SERIES"))
                .filter(s -> !s.isBlank())
                .map(s -> Arrays.stream(s.split(","))
                        .map(String::trim)
                        .filter(id -> !id.isEmpty())
                        .toList())
                .orElse(DEFAULT_SERIES);

        Optional<LocalDate> observationStart = Optional.ofNullable(env.get("FRED_OBSERVATION_START"))
                .filter(s -> !s.isBlank())
                .map(LocalDate::parse);

        Path rawDataDir = Path.of(env.getOrDefault("RAW_DATA_DIR", "data/raw"));

        return new IngestionConfig(apiKey.trim(), seriesIds, observationStart, rawDataDir);
    }
}
