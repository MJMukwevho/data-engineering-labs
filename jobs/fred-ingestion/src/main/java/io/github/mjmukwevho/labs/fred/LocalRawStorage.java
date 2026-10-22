package io.github.mjmukwevho.labs.fred;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/** Raw layer on the local filesystem, for running without the object store. */
public class LocalRawStorage implements RawStorage {

    private final Path root;

    public LocalRawStorage(Path root) {
        this.root = root;
    }

    @Override
    public String put(String relativePath, String content) throws IOException {
        Path target = root.resolve(relativePath);
        Files.createDirectories(target.getParent());

        // Write to a temp file first so a failed run never leaves a half-written file behind
        Path tmp = target.resolveSibling(target.getFileName() + ".tmp");
        Files.writeString(tmp, content, StandardCharsets.UTF_8);
        Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        return target.toString();
    }
}
