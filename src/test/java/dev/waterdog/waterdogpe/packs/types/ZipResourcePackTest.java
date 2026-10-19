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

package dev.waterdog.waterdogpe.packs.types;

import dev.waterdog.waterdogpe.packs.TestPacks;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * manifest.json is looked up at the zip root first, then inside a single top-level folder.
 */
public class ZipResourcePackTest {

    private static final UUID ROOT_ID = UUID.fromString("0b6d1f7e-1c2a-4f3b-9d4e-5a6b7c8d9e01");
    private static final UUID FOLDER_ID = UUID.fromString("1c7e2a8f-2d3b-4a4c-8e5f-6b7c8d9e0f12");

    @TempDir
    Path tempDir;

    @Test
    public void rootManifestLoads() throws IOException {
        Map<String, byte[]> entries = new LinkedHashMap<>();
        entries.put("manifest.json", TestPacks.manifest("Root", ROOT_ID));
        entries.put("textures/a.png", new byte[]{1});

        ZipResourcePack pack = this.load(entries);
        try {
            assertTrue(pack.loadManifest());
            assertEquals("", pack.getContentRoot());
            assertEquals(ROOT_ID, pack.getPackId());
        } finally {
            pack.close();
        }
    }

    @Test
    public void manifestInsideSingleTopLevelFolderLoads() throws IOException {
        Map<String, byte[]> entries = new LinkedHashMap<>();
        entries.put("MyPack/", new byte[0]);
        entries.put("MyPack/manifest.json", TestPacks.manifest("Folder", FOLDER_ID));
        entries.put("MyPack/textures/a.png", "png".getBytes(StandardCharsets.UTF_8));

        ZipResourcePack pack = this.load(entries);
        try {
            assertTrue(pack.loadManifest());
            assertTrue(pack.getPackManifest().validate());
            assertEquals("MyPack/", pack.getContentRoot());
            assertEquals(FOLDER_ID, pack.getPackId());
            // Nested paths resolve with forward slashes inside the folder, also on Windows.
            try (InputStream stream = pack.getStream(Paths.get("textures", "a.png"))) {
                assertNotNull(stream);
                assertEquals("png", new String(stream.readAllBytes(), StandardCharsets.UTF_8));
            }
        } finally {
            pack.close();
        }
    }

    @Test
    public void rootManifestWinsOverFolderManifest() throws IOException {
        Map<String, byte[]> entries = new LinkedHashMap<>();
        entries.put("Inner/manifest.json", TestPacks.manifest("Folder", FOLDER_ID));
        entries.put("manifest.json", TestPacks.manifest("Root", ROOT_ID));

        ZipResourcePack pack = this.load(entries);
        try {
            assertTrue(pack.loadManifest());
            assertEquals("", pack.getContentRoot());
            assertEquals(ROOT_ID, pack.getPackId());
        } finally {
            pack.close();
        }
    }

    @Test
    public void macosMetadataFolderIsIgnored() throws IOException {
        Map<String, byte[]> entries = new LinkedHashMap<>();
        entries.put("__MACOSX/MyPack/._manifest.json", new byte[]{0});
        entries.put("MyPack/manifest.json", TestPacks.manifest("Folder", FOLDER_ID));

        ZipResourcePack pack = this.load(entries);
        try {
            assertTrue(pack.loadManifest());
            assertEquals("MyPack/", pack.getContentRoot());
        } finally {
            pack.close();
        }
    }

    @Test
    public void severalTopLevelFoldersAreNotGuessed() throws IOException {
        Map<String, byte[]> entries = new LinkedHashMap<>();
        entries.put("PackA/manifest.json", TestPacks.manifest("A", ROOT_ID));
        entries.put("PackB/manifest.json", TestPacks.manifest("B", FOLDER_ID));

        ZipResourcePack pack = this.load(entries);
        try {
            assertFalse(pack.loadManifest());
        } finally {
            pack.close();
        }
    }

    @Test
    public void folderNextToRootFilesIsNotUsed() throws IOException {
        Map<String, byte[]> entries = new LinkedHashMap<>();
        entries.put("pack_icon.png", new byte[]{1});
        entries.put("MyPack/manifest.json", TestPacks.manifest("Folder", FOLDER_ID));

        ZipResourcePack pack = this.load(entries);
        try {
            assertFalse(pack.loadManifest());
        } finally {
            pack.close();
        }
    }

    private ZipResourcePack load(Map<String, byte[]> entries) throws IOException {
        Path file = TestPacks.writeZip(this.tempDir.resolve("pack.zip"), entries);
        return new ZipResourcePack(file);
    }
}
