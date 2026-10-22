package io.github.mjmukwevho.labs.fred;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
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
        assertEquals(Path.of("data/raw"), config.rawDataDir());
    }

    @Test
    void readsOverrides() {
        IngestionConfig config = IngestionConfig.fromEnvironment(Map.of(
                "FRED_API_KEY", "abc",
                "FRED_SERIES", " SP500 , DGS10,,",
                "FRED_OBSERVATION_START", "2024-01-01",
                "RAW_DATA_DIR", "/lake/raw"));

        assertEquals(List.of("SP500", "DGS10"), config.seriesIds());
        assertEquals(Optional.of(LocalDate.of(2024, 1, 1)), config.observationStart());
        assertEquals(Path.of("/lake/raw"), config.rawDataDir());
    }

    @Test
    void rejectsMissingApiKey() {
        assertThrows(IllegalArgumentException.class, () -> IngestionConfig.fromEnvironment(Map.of()));
        assertThrows(IllegalArgumentException.class,
                () -> IngestionConfig.fromEnvironment(Map.of("FRED_API_KEY", "  ")));
    }
}
