/*
 * Copyright 2026 WaterdogTEAM
 * Licensed under the GNU General Public License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.waterdog.waterdogpe.packs;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import java.util.zip.CRC32;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Builds small pack zips on disk for tests.
 */
public final class TestPacks {

    private TestPacks() {
    }

    public static byte[] manifest(String name, UUID uuid) {
        String json = "{\"format_version\":2,"
                + "\"header\":{\"name\":\"" + name + "\",\"description\":\"test\",\"uuid\":\"" + uuid + "\",\"version\":[1,0,0]},"
                + "\"modules\":[{\"type\":\"resources\",\"uuid\":\"" + UUID.randomUUID() + "\",\"version\":[1,0,0]}]}";
        return json.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Writes the given entries, in iteration order, stored uncompressed so the zip size follows the
     * content size.
     */
    public static Path writeZip(Path file, Map<String, byte[]> entries) throws IOException {
        try (OutputStream out = Files.newOutputStream(file); ZipOutputStream zip = new ZipOutputStream(out)) {
            for (Map.Entry<String, byte[]> entry : entries.entrySet()) {
                byte[] data = entry.getValue();
                ZipEntry zipEntry = new ZipEntry(entry.getKey());
                zipEntry.setMethod(ZipEntry.STORED);
                zipEntry.setSize(data.length);
                CRC32 crc = new CRC32();
                crc.update(data);
                zipEntry.setCrc(crc.getValue());
                zip.putNextEntry(zipEntry);
                zip.write(data);
                zip.closeEntry();
            }
        }
        return file;
    }
}
