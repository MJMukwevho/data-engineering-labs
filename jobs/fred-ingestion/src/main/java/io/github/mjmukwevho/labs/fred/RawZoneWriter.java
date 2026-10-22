package io.github.mjmukwevho.labs.fred;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Optional;

/**
 * Writes source responses into the raw layer using Hive-style partitions:
 *
 * <pre>
 * {rawDataDir}/fred/series_id={id}/ingest_date={yyyy-MM-dd}/observations.json
 * {rawDataDir}/fred/series_id={id}/ingest_date={yyyy-MM-dd}/_metadata.json
 * </pre>
 *
 * The same layout works unchanged on S3, where Glue and Athena can discover the partitions.
 * Re-running on the same day overwrites that day's partition, so the job is idempotent.
 */
public class RawZoneWriter {

    private final Path rawDataDir;

    public RawZoneWriter(Path rawDataDir) {
        this.rawDataDir = rawDataDir;
    }

    public Path write(String seriesId, String body, Optional<LocalDate> observationStart, Instant ingestedAt)
            throws IOException {
        LocalDate ingestDate = LocalDate.ofInstant(ingestedAt, ZoneOffset.UTC);
        Path partition = partitionPath(seriesId, ingestDate);
        Files.createDirectories(partition);

        Path dataFile = partition.resolve("observations.json");
        writeAtomically(dataFile, body);

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
        writeAtomically(partition.resolve("_metadata.json"), metadata);

        return dataFile;
    }

    Path partitionPath(String seriesId, LocalDate ingestDate) {
        return rawDataDir
                .resolve("fred")
                .resolve("series_id=" + seriesId)
                .resolve("ingest_date=" + ingestDate);
    }

    /** Writes to a temp file first so a failed run never leaves a half-written file behind. */
    private static void writeAtomically(Path target, String content) throws IOException {
        Path tmp = target.resolveSibling(target.getFileName() + ".tmp");
        Files.writeString(tmp, content, StandardCharsets.UTF_8);
        Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }
}
