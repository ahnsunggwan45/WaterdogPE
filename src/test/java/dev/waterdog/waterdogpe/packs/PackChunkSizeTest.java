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

import dev.waterdog.waterdogpe.ProxyServer;
import dev.waterdog.waterdogpe.event.EventManager;
import dev.waterdog.waterdogpe.logger.MainLogger;
import dev.waterdog.waterdogpe.packs.types.ZipResourcePack;
import dev.waterdog.waterdogpe.utils.config.proxy.ProxyConfig;
import org.cloudburstmc.protocol.bedrock.packet.ResourcePackChunkDataPacket;
import org.cloudburstmc.protocol.bedrock.packet.ResourcePackChunkRequestPacket;
import org.cloudburstmc.protocol.bedrock.packet.ResourcePackDataInfoPacket;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The in-protocol chunk size was 256 KiB when mobile clients crashed while downloading packs, and
 * upstream merges reverted the 100 KiB fork patch twice. These tests pin the default to 100 KiB.
 */
public class PackChunkSizeTest {

    private static final UUID PACK_ID = UUID.fromString("2d8f3b9a-3e4c-4b5d-9f60-7c8d9e0f1a23");

    @TempDir
    Path tempDir;

    private final ProxyServer proxy = mock(ProxyServer.class);
    private final ProxyConfig config = mock(ProxyConfig.class);
    private PackManager packManager;

    @BeforeEach
    public void setUp() {
        when(this.proxy.getConfiguration()).thenReturn(this.config);
        when(this.proxy.getLogger()).thenReturn(mock(MainLogger.class));
        when(this.proxy.getEventManager()).thenReturn(mock(EventManager.class));
        when(this.config.getPackChunkSize()).thenReturn(PackManager.DEFAULT_CHUNK_SIZE);
        this.packManager = new PackManager(this.proxy);
    }

    @AfterEach
    public void tearDown() {
        this.packManager.clear();
    }

    @Test
    public void defaultChunkSizeIs100KiB() {
        assertEquals(102400, PackManager.DEFAULT_CHUNK_SIZE);
        assertEquals(102400, new PackManager(this.proxy).getChunkSize());
    }

    @Test
    public void configDefaultChunkSizeIs100KiB() {
        assertEquals(102400, new ProxyConfig(new File("unused-config.yml")).getPackChunkSize());
    }

    @Test
    public void chunkSizeIsClamped() {
        assertEquals(PackManager.DEFAULT_CHUNK_SIZE, PackManager.clampChunkSize(0));
        assertEquals(PackManager.DEFAULT_CHUNK_SIZE, PackManager.clampChunkSize(-1));
        assertEquals(PackManager.MIN_CHUNK_SIZE, PackManager.clampChunkSize(1));
        assertEquals(PackManager.MAX_CHUNK_SIZE, PackManager.clampChunkSize(4 * 1024 * 1024));
        assertEquals(64 * 1024, PackManager.clampChunkSize(64 * 1024));
    }

    @Test
    public void dataInfoAndChunksUseTheChunkSize() throws IOException {
        Path zip = this.writePack(this.tempDir.resolve("pack.zip"), 250 * 1024);
        long packSize = Files.size(zip);
        ZipResourcePack pack = new ZipResourcePack(zip);
        assertTrue(pack.loadManifest());
        pack.setContentKey("");
        assertTrue(this.packManager.registerPack(pack));

        String idVer = PACK_ID + "_1.0.0";
        ResourcePackDataInfoPacket info = this.packManager.packInfoFromIdVer(idVer);
        assertEquals(102400, info.getMaxChunkSize());
        assertEquals((packSize + 102399) / 102400, info.getChunkCount());

        ResourcePackChunkDataPacket second = this.packManager.packChunkDataPacket(idVer, chunkRequest(1));
        assertEquals(102400, second.getProgress());
        assertEquals(102400, second.getData().readableBytes());

        int last = (int) info.getChunkCount() - 1;
        ResourcePackChunkDataPacket tail = this.packManager.packChunkDataPacket(idVer, chunkRequest(last));
        assertEquals(packSize - 102400L * last, tail.getData().readableBytes());
    }

    @Test
    public void configuredChunkSizeIsReadAndClampedOnLoad() throws IOException {
        Path packs = Files.createDirectories(this.tempDir.resolve("packs"));
        this.writePack(packs.resolve("pack.zip"), 1024);
        when(this.config.getPackChunkSize()).thenReturn(1024 * 1024);

        this.packManager.loadPacks(packs);
        assertEquals(PackManager.MAX_CHUNK_SIZE, this.packManager.getChunkSize());
        assertEquals(1, this.packManager.getPacks().size());
    }

    @Test
    public void localPackWrappedInFolderLoads() throws IOException {
        Path packs = Files.createDirectories(this.tempDir.resolve("packs"));
        Map<String, byte[]> entries = new LinkedHashMap<>();
        entries.put("Wrapped/manifest.json", TestPacks.manifest("Wrapped", PACK_ID));
        TestPacks.writeZip(packs.resolve("wrapped.mcpack"), entries);

        this.packManager.loadPacks(packs);
        assertNotNull(this.packManager.getPacks().get(PACK_ID));
    }

    private Path writePack(Path file, int payloadSize) throws IOException {
        byte[] payload = new byte[payloadSize];
        new Random(1).nextBytes(payload);
        Map<String, byte[]> entries = new LinkedHashMap<>();
        entries.put("manifest.json", TestPacks.manifest("Test", PACK_ID));
        entries.put("payload.bin", payload);
        return TestPacks.writeZip(file, entries);
    }

    private static ResourcePackChunkRequestPacket chunkRequest(int chunkIndex) {
        ResourcePackChunkRequestPacket request = new ResourcePackChunkRequestPacket();
        request.setPackId(PACK_ID);
        request.setPackVersion("1.0.0");
        request.setChunkIndex(chunkIndex);
        return request;
    }
}
