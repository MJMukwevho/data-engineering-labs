package io.github.mjmukwevho.labs.fred;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class IngestionConfigTest {

    @Test
    void usesDefaultsWhenOnlyApiKeyIsSet() {
        IngestionConfig config = IngestionConfig.fromEnvironment(Map.of("FRED_API_KEY", "abc"));

        assertEquals("abc", config.apiKey());
        assertEquals(IngestionConfig.DEFAULT_SERIES, config.seriesIds());
        assertEquals(Optional.empty(), config.observationStart());
        assertEquals("data/raw", config.rawLocation());
        assertFalse(config.s3ForcePathStyle());
        assertEquals(FredClient.DEFAULT_BASE_URL, config.fredBaseUrl());
    }

    @Test
    void readsOverrides() {
        IngestionConfig config = IngestionConfig.fromEnvironment(Map.of(
                "FRED_API_KEY", "abc",
                "FRED_SERIES", " SP500 , DGS10,,",
                "FRED_OBSERVATION_START", "2024-01-01",
                "RAW_DATA_LOCATION", "s3://lake/raw",
                "S3_FORCE_PATH_STYLE", "true",
                "FRED_BASE_URL", "http://localhost:8000/fred"));

        assertEquals(List.of("SP500", "DGS10"), config.seriesIds());
        assertEquals(Optional.of(LocalDate.of(2024, 1, 1)), config.observationStart());
        assertEquals("s3://lake/raw", config.rawLocation());
        assertTrue(config.s3ForcePathStyle());
        assertEquals("http://localhost:8000/fred", config.fredBaseUrl());
    }

    @Test
    void acceptsOlderRawDataDirName() {
        IngestionConfig config = IngestionConfig.fromEnvironment(Map.of(
                "FRED_API_KEY", "abc",
                "RAW_DATA_DIR", "/lake/raw"));

        assertEquals("/lake/raw", config.rawLocation());
    }

    @Test
    void rejectsMissingApiKey() {
        assertThrows(IllegalArgumentException.class, () -> IngestionConfig.fromEnvironment(Map.of()));
        assertThrows(IllegalArgumentException.class,
                () -> IngestionConfig.fromEnvironment(Map.of("FRED_API_KEY", "  ")));
    }
}
