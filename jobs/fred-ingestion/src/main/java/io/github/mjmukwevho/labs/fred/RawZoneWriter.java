package io.github.mjmukwevho.labs.fred;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Optional;

/**
 * Writes source responses into the raw layer using Hive-style partitions:
 *
 * <pre>
 * {root}/fred/series_id={id}/ingest_date={yyyy-MM-dd}/observations.json
 * {root}/fred/series_id={id}/ingest_date={yyyy-MM-dd}/_metadata.json
 * </pre>
 *
 * The root is a local folder or an S3 location (see {@link RawStorage}); Glue and Athena
 * can discover these partitions on S3. Re-running on the same day overwrites that day's
 * partition, so the job is idempotent. The metadata file is written last, so its presence
 * means the partition is complete.
 */
public class RawZoneWriter {

    private final RawStorage storage;

    public RawZoneWriter(RawStorage storage) {
        this.storage = storage;
    }

    /** Returns the location of the data file. */
    public String write(String seriesId, String body, Optional<LocalDate> observationStart, Instant ingestedAt)
            throws IOException {
        String partition = partitionPath(seriesId, LocalDate.ofInstant(ingestedAt, ZoneOffset.UTC));

        String dataLocation = storage.put(partition + "/observations.json", body);

        // Lineage: where the data came from and when. The API key is deliberately left out.
        String metadata = """
                {
                  "source": "FRED",
                  "endpoint": "series/observations",
                  "series_id": "%s",
                  "observation_start": %s,
                  "ingested_at": "%s",
                  "bytes": %d
                }
                """.formatted(
                seriesId,
                observationStart.map(d -> "\"" + d + "\"").orElse("null"),
                ingestedAt,
                body.getBytes(StandardCharsets.UTF_8).length);
        storage.put(partition + "/_metadata.json", metadata);

        return dataLocation;
    }

    static String partitionPath(String seriesId, LocalDate ingestDate) {
        return "fred/series_id=" + seriesId + "/ingest_date=" + ingestDate;
    }
}
