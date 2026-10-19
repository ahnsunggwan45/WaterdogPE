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

package dev.waterdog.waterdogpe.network.protocol.handler.upstream;

import dev.waterdog.waterdogpe.ProxyServer;
import dev.waterdog.waterdogpe.logger.MainLogger;
import dev.waterdog.waterdogpe.network.connection.peer.BedrockServerSession;
import dev.waterdog.waterdogpe.packs.PackManager;
import dev.waterdog.waterdogpe.player.ProxiedPlayer;
import io.netty.buffer.Unpooled;
import org.cloudburstmc.protocol.bedrock.packet.ResourcePackChunkDataPacket;
import org.cloudburstmc.protocol.bedrock.packet.ResourcePackChunkRequestPacket;
import org.cloudburstmc.protocol.bedrock.packet.ResourcePackClientResponsePacket;
import org.cloudburstmc.protocol.bedrock.packet.ResourcePackDataInfoPacket;
import org.cloudburstmc.protocol.bedrock.packet.ResourcePacksInfoPacket;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The [PackCDN] and [PackPhase] lines are read by log tooling as space separated key=value tokens.
 */
class ResourcePacksHandlerDiagnosticsTest {

    private static final UUID PACK_ID = UUID.fromString("6f1c2a3b-4d5e-4f60-8172-839405a6b7c8");
    private static final String VERSION = "1.0.0";
    private static final String PACK_ID_VER = PACK_ID + "_" + VERSION;
    private static final String CDN_URL = "https://cdn.example/pack.zip";
    private static final int CHUNK_COUNT = 3;
    private static final int CHUNK_BYTES = 1024;

    private final ProxiedPlayer player = mock(ProxiedPlayer.class);
    private final PackManager packManager = mock(PackManager.class);
    private final MainLogger logger = mock(MainLogger.class);
    private final List<String> lines = new ArrayList<>();
    private final ResourcePackDataInfoPacket info = new ResourcePackDataInfoPacket();

    @BeforeEach
    void setUp() {
        ProxyServer proxy = mock(ProxyServer.class);
        when(this.player.isConnected()).thenReturn(true);
        when(this.player.getProxy()).thenReturn(proxy);
        when(this.player.getName()).thenReturn("Steve Two");
        when(this.player.getLogger()).thenReturn(this.logger);
        when(proxy.getPackManager()).thenReturn(this.packManager);
        doAnswer(inv -> this.lines.add(inv.getArgument(0))).when(this.logger).info(anyString());

        this.info.setPackId(PACK_ID);
        this.info.setPackVersion(VERSION);
        this.info.setChunkCount(CHUNK_COUNT);
        when(this.packManager.packInfoFromIdVer(PACK_ID_VER)).thenReturn(this.info);
        when(this.packManager.packChunkDataPacket(anyString(), any())).thenAnswer(inv -> {
            ResourcePackChunkDataPacket data = new ResourcePackChunkDataPacket();
            data.setChunkIndex(inv.<ResourcePackChunkRequestPacket>getArgument(1).getChunkIndex());
            data.setData(Unpooled.wrappedBuffer(new byte[CHUNK_BYTES]));
            return data;
        });
    }

    @Test
    void cdnPackFetchedInProtocolIsLoggedOncePerPack() {
        ResourcePacksHandler handler = new ResourcePacksHandler(this.player, infoPacket(CDN_URL));
        this.requestPack(handler);
        this.requestAllChunks(handler);
        this.requestPack(handler);

        List<String> cdnLines = this.linesTagged("[PackCDN]");
        assertEquals(1, cdnLines.size());
        String line = cdnLines.get(0);
        assertTrue(line.contains(" player=\"Steve Two\" "), line);
        assertTrue(line.contains(" pack=" + PACK_ID + " "), line);
        assertTrue(line.contains(" url=" + CDN_URL + " "), line);
        assertTrue(line.contains(" event=chunk_fallback "), line);
    }

    @Test
    void packWithoutCdnUrlIsNotLoggedAsCdnFallback() {
        ResourcePacksHandler handler = new ResourcePacksHandler(this.player, infoPacket(null));
        this.requestPack(handler);
        this.requestAllChunks(handler);

        assertEquals(0, this.linesTagged("[PackCDN]").size());
    }

    @Test
    void completedLogsPhaseOnceWithChunkCounts() {
        ResourcePacksHandler handler = new ResourcePacksHandler(this.player, infoPacket(CDN_URL));
        this.requestPack(handler);
        this.requestAllChunks(handler);
        this.respond(handler, ResourcePackClientResponsePacket.Status.COMPLETED);
        this.respond(handler, ResourcePackClientResponsePacket.Status.COMPLETED);

        List<String> phaseLines = this.linesTagged("[PackPhase]");
        assertEquals(1, phaseLines.size());
        String line = phaseLines.get(0);
        assertTrue(line.contains(" result=done "), line);
        assertTrue(line.contains(" ms="), line);
        assertTrue(line.contains(" cdn=1 chunk=1 cdnChunk=1 "), line);
        assertTrue(line.contains(" chunks=" + CHUNK_COUNT + " "), line);
        assertTrue(line.endsWith(" chunkBytes=" + CHUNK_COUNT * CHUNK_BYTES), line);
    }

    @Test
    void disconnectDuringPackStageLogsPhaseOnce() {
        BedrockServerSession connection = mock(BedrockServerSession.class);
        List<Consumer<CharSequence>> listeners = new ArrayList<>();
        doAnswer(inv -> listeners.add(inv.getArgument(0))).when(connection).addDisconnectListener(any());
        when(this.player.getConnection()).thenReturn(connection);
        ResourcePacksHandler handler = new ResourcePacksHandler(this.player, infoPacket(null));

        listeners.forEach(listener -> listener.accept("timed out"));
        this.respond(handler, ResourcePackClientResponsePacket.Status.COMPLETED);

        List<String> phaseLines = this.linesTagged("[PackPhase]");
        assertEquals(1, phaseLines.size());
        assertTrue(phaseLines.get(0).contains(" result=disconnect "), phaseLines.get(0));
        assertTrue(phaseLines.get(0).endsWith(" reason=\"timed out\""), phaseLines.get(0));
    }

    @Test
    void valuesWithSpacesOrQuotesStayOneToken() {
        assertEquals("Steve", ResourcePacksHandler.value("Steve"));
        assertEquals("\"Steve Two\"", ResourcePacksHandler.value("Steve Two"));
        assertEquals("\"a'b\"", ResourcePacksHandler.value("a\"b"));
        assertEquals("\"\"", ResourcePacksHandler.value(""));
    }

    private List<String> linesTagged(String tag) {
        return this.lines.stream().filter(line -> line.startsWith(tag)).toList();
    }

    private void requestPack(ResourcePacksHandler handler) {
        ResourcePackClientResponsePacket response = new ResourcePackClientResponsePacket();
        response.setStatus(ResourcePackClientResponsePacket.Status.SEND_PACKS);
        response.getPackIds().add(PACK_ID_VER);
        handler.handle(response);
    }

    private void requestAllChunks(ResourcePacksHandler handler) {
        for (int index = 0; index < CHUNK_COUNT; index++) {
            ResourcePackChunkRequestPacket request = new ResourcePackChunkRequestPacket();
            request.setPackId(PACK_ID);
            request.setPackVersion(VERSION);
            request.setChunkIndex(index);
            handler.handle(request);
        }
    }

    private void respond(ResourcePacksHandler handler, ResourcePackClientResponsePacket.Status status) {
        ResourcePackClientResponsePacket response = new ResourcePackClientResponsePacket();
        response.setStatus(status);
        handler.handle(response);
    }

    private static ResourcePacksInfoPacket infoPacket(String cdnUrl) {
        ResourcePacksInfoPacket packet = new ResourcePacksInfoPacket();
        packet.getResourcePackInfos().add(new ResourcePacksInfoPacket.Entry(PACK_ID, VERSION, 3L * CHUNK_BYTES,
                "", "", "", false, false, false, cdnUrl));
        return packet;
    }
}
