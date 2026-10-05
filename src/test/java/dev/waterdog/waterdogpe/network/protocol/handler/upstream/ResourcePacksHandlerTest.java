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
import dev.waterdog.waterdogpe.packs.PackManager;
import dev.waterdog.waterdogpe.player.ProxiedPlayer;
import dev.waterdog.waterdogpe.scheduler.TaskHandler;
import dev.waterdog.waterdogpe.scheduler.WaterdogScheduler;
import dev.waterdog.waterdogpe.transfer.TransferTestHarness;
import org.cloudburstmc.protocol.bedrock.packet.ResourcePackChunkDataPacket;
import org.cloudburstmc.protocol.bedrock.packet.ResourcePackChunkRequestPacket;
import org.cloudburstmc.protocol.bedrock.packet.ResourcePackClientResponsePacket;
import org.cloudburstmc.protocol.bedrock.packet.ResourcePackDataInfoPacket;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Chunk requests must name a pack this client was offered and a chunk inside it, and the
 * pending queue must stay bounded so a client cannot grow it without limit.
 */
public class ResourcePacksHandlerTest {

    private static final UUID PACK_ID = UUID.fromString("6f1c2a3b-4d5e-4f60-8172-839405a6b7c8");
    private static final String PACK_VERSION = "1.0.0";
    private static final String PACK_ID_VER = PACK_ID + "_" + PACK_VERSION;
    private static final int CHUNK_COUNT = 3;
    // MAX_PENDING_REQUESTS_PER_CHUNK in ResourcePacksHandler
    private static final int PENDING_LIMIT = CHUNK_COUNT * 4;

    private final ProxyServer proxy = mock(ProxyServer.class);
    private final WaterdogScheduler scheduler = mock(WaterdogScheduler.class);
    private final PackManager packManager = mock(PackManager.class);
    private final ProxiedPlayer player = mock(ProxiedPlayer.class);
    @SuppressWarnings("unchecked")
    private final TaskHandler<Runnable> sendTask = mock(TaskHandler.class);
    private final List<Runnable> scheduledTasks = new ArrayList<>();
    private final ResourcePackDataInfoPacket info = new ResourcePackDataInfoPacket();

    private ResourcePacksHandler handler;

    @BeforeEach
    public void setUp() {
        when(this.proxy.getScheduler()).thenReturn(this.scheduler);
        when(this.proxy.getPackManager()).thenReturn(this.packManager);
        when(this.scheduler.scheduleRepeating(any(Runnable.class), anyInt())).thenAnswer(invocation -> {
            this.scheduledTasks.add(invocation.getArgument(0));
            return this.sendTask;
        });

        this.info.setPackId(PACK_ID);
        this.info.setPackVersion(PACK_VERSION);
        this.info.setMaxChunkSize(1024);
        this.info.setChunkCount(CHUNK_COUNT);
        when(this.packManager.packInfoFromIdVer(PACK_ID_VER)).thenReturn(this.info);
        when(this.packManager.packChunkDataPacket(anyString(), any(ResourcePackChunkRequestPacket.class))).thenAnswer(invocation -> {
            ResourcePackChunkRequestPacket request = invocation.getArgument(1);
            ResourcePackChunkDataPacket data = new ResourcePackChunkDataPacket();
            data.setPackId(request.getPackId());
            data.setPackVersion(request.getPackVersion());
            data.setChunkIndex(request.getChunkIndex());
            return data;
        });

        when(this.player.getProxy()).thenReturn(this.proxy);
        when(this.player.isConnected()).thenReturn(true);
        when(this.player.getLogger()).thenReturn(mock(MainLogger.class));

        TransferTestHarness.setProxyInstance(this.proxy);
        this.handler = new ResourcePacksHandler(this.player);
    }

    @AfterEach
    public void tearDown() {
        TransferTestHarness.setProxyInstance(null);
    }

    @Test
    public void requestForPackNotOfferedIsIgnored() {
        this.handler.handle(chunkRequest(0));

        assertEquals(0, this.scheduledTasks.size());
        verify(this.player, never()).disconnect(any(CharSequence.class));
    }

    @Test
    public void requestOutsideOfferedChunksIsIgnored() {
        this.offerPack(PACK_ID_VER);

        this.handler.handle(chunkRequest(-1));
        this.handler.handle(chunkRequest(CHUNK_COUNT));
        this.handler.handle(chunkRequest(Integer.MAX_VALUE));
        assertEquals(0, this.scheduledTasks.size());

        this.handler.handle(chunkRequest(0));
        assertEquals(1, this.scheduledTasks.size());
        verify(this.player, never()).disconnect(any(CharSequence.class));
    }

    @Test
    public void floodingChunkRequestsDisconnects() {
        this.offerPack(PACK_ID_VER);

        for (int i = 0; i < PENDING_LIMIT; i++) {
            this.handler.handle(chunkRequest(i % CHUNK_COUNT));
        }
        verify(this.player, never()).disconnect(any(CharSequence.class));

        this.handler.handle(chunkRequest(0));
        verify(this.player).disconnect((CharSequence) "disconnectionScreen.resourcePack");
        verify(this.sendTask).cancel();
    }

    @Test
    public void validRequestsAreServedInOrder() {
        this.offerPack(PACK_ID_VER);
        for (int i = 0; i < CHUNK_COUNT; i++) {
            this.handler.handle(chunkRequest(i));
        }

        Runnable sendNextChunk = this.scheduledTasks.get(0);
        for (int i = 0; i < CHUNK_COUNT; i++) {
            sendNextChunk.run();
        }

        verify(this.player, times(CHUNK_COUNT)).sendPacket(any(ResourcePackChunkDataPacket.class));
        verify(this.player, never()).disconnect(any(CharSequence.class));
    }

    @Test
    public void repeatedPackIdIsOfferedOnce() {
        this.offerPack(PACK_ID_VER, PACK_ID_VER);
        this.offerPack(PACK_ID_VER);

        verify(this.player, times(1)).sendPacket(this.info);

        // The pending limit is still based on a single copy of the pack.
        for (int i = 0; i <= PENDING_LIMIT; i++) {
            this.handler.handle(chunkRequest(i % CHUNK_COUNT));
        }
        verify(this.player).disconnect((CharSequence) "disconnectionScreen.resourcePack");
    }

    private void offerPack(String... packIdVers) {
        ResourcePackClientResponsePacket response = new ResourcePackClientResponsePacket();
        response.setStatus(ResourcePackClientResponsePacket.Status.SEND_PACKS);
        response.getPackIds().addAll(List.of(packIdVers));
        this.handler.handle(response);
    }

    private static ResourcePackChunkRequestPacket chunkRequest(int chunkIndex) {
        ResourcePackChunkRequestPacket request = new ResourcePackChunkRequestPacket();
        request.setPackId(PACK_ID);
        request.setPackVersion(PACK_VERSION);
        request.setChunkIndex(chunkIndex);
        return request;
    }
}
