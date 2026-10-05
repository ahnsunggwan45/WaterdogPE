/*
 * Copyright 2022 WaterdogTEAM
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
import it.unimi.dsi.fastutil.longs.LongArraySet;
import it.unimi.dsi.fastutil.longs.LongSet;
import org.cloudburstmc.protocol.bedrock.packet.*;
import dev.waterdog.waterdogpe.event.defaults.PlayerResourcePackApplyEvent;
import dev.waterdog.waterdogpe.packs.PackManager;
import dev.waterdog.waterdogpe.player.ProxiedPlayer;
import dev.waterdog.waterdogpe.scheduler.TaskHandler;
import org.cloudburstmc.protocol.common.PacketSignal;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Upstream handler handling proxy manager resource packs.
 */
public class ResourcePacksHandler extends AbstractUpstreamHandler {

    // Send one queued chunk every this many ticks instead of bursting them out.
    private static final int CHUNK_SEND_PERIOD = 4;
    // Pending requests allowed per offered chunk, leaving room for re-requests but not floods.
    private static final int MAX_PENDING_REQUESTS_PER_CHUNK = 4;

    private final Queue<ResourcePackDataInfoPacket> pendingPacks = new LinkedList<>();
    // LinkedBlockingQueue for its constant time size().
    private final Queue<ResourcePackChunkRequestPacket> pendingChunks = new LinkedBlockingQueue<>();
    // Packs offered to this client by id_version; only touched from packet handlers.
    private final Map<String, ResourcePackDataInfoPacket> offeredPacks = new HashMap<>();
    private long offeredChunks;
    private final LongSet sentChunks = new LongArraySet();
    private ResourcePackDataInfoPacket sendingPack;
    private TaskHandler<Runnable> sendTask;

    public ResourcePacksHandler(ProxiedPlayer player) {
        super(player);
    }

    @Override
    public PacketSignal handle(ResourcePackClientResponsePacket packet) {
        PackManager packManager = this.player.getProxy().getPackManager();

        switch (packet.getStatus()) {
            case REFUSED:
                this.player.disconnect("disconnectionScreen.noReason");
                break;
            case SEND_PACKS:
                for (String packIdVer : packet.getPackIds()) {
                    ResourcePackDataInfoPacket response = packManager.packInfoFromIdVer(packIdVer);
                    if (response == null) {
                        this.player.disconnect("disconnectionScreen.resourcePack");
                        break;
                    }
                    // Queue each pack once; ids repeated within or across SEND_PACKS would grow the queue.
                    if (this.offeredPacks.putIfAbsent(packIdVer, response) == null) {
                        this.offeredChunks += response.getChunkCount();
                        this.pendingPacks.offer(response);
                    }
                }
                this.sendNextPacket();
                break;
            case HAVE_ALL_PACKS:
                PlayerResourcePackApplyEvent event = new PlayerResourcePackApplyEvent(this.player, packManager.getStackPacket());
                this.player.getProxy().getEventManager().callEvent(event);
                this.player.getConnection().sendPacket(event.getStackPacket());
                break;
            case COMPLETED:
                this.stopSending();
                if (!this.player.hasUpstreamBridge()) {
                    this.player.initialConnect(); // First connection
                }
                break;
        }

        return this.cancel();
    }

    @Override
    public PacketSignal handle(ResourcePackChunkRequestPacket packet) {
        if (!this.player.isConnected()) {
            return this.cancel();
        }

        ResourcePackDataInfoPacket info = this.offeredPacks.get(packet.getPackId() + "_" + packet.getPackVersion());
        if (info == null || packet.getChunkIndex() < 0 || packet.getChunkIndex() >= info.getChunkCount()) {
            this.player.getLogger().debug("[{}|{}] Ignoring invalid request for chunk {} of resource pack {}_{}",
                    this.player.getAddress(), this.player.getName(), packet.getChunkIndex(), packet.getPackId(), packet.getPackVersion());
            return this.cancel();
        }

        if (this.pendingChunks.size() >= this.offeredChunks * MAX_PENDING_REQUESTS_PER_CHUNK) {
            this.player.getLogger().warning("[{}|{}] Sent too many resource pack chunk requests ({} pending), disconnecting",
                    this.player.getAddress(), this.player.getName(), this.pendingChunks.size());
            this.stopSending();
            this.pendingChunks.clear();
            this.player.disconnect("disconnectionScreen.resourcePack");
            return this.cancel();
        }

        this.queueChunk(packet);
        return this.cancel();
    }

    private void queueChunk(ResourcePackChunkRequestPacket packet) {
        this.pendingChunks.offer(packet);
        if (this.sendTask == null) {
            this.sendTask = ProxyServer.getInstance().getScheduler().scheduleRepeating(this::sendNextChunk, CHUNK_SEND_PERIOD);
        }
    }

    private void sendNextChunk() {
        if (!this.player.isConnected()) {
            this.stopSending();
            return;
        }

        ResourcePackChunkRequestPacket request = this.pendingChunks.poll();
        if (request == null) {
            return; // Nothing queued this tick; keep the loop alive for the next chunk request.
        }

        PackManager packManager = this.player.getProxy().getPackManager();
        ResourcePackChunkDataPacket response = packManager.packChunkDataPacket(request.getPackId() + "_" + request.getPackVersion(), request);
        if (response == null) {
            this.player.disconnect("Unknown resource pack!");
            this.stopSending();
            return;
        }

        this.player.sendPacket(response);
        this.sentChunks.add(response.getChunkIndex());
        if (this.sendingPack != null && this.sentChunks.size() >= this.sendingPack.getChunkCount()) {
            this.sendNextPacket();
        }
    }

    private void stopSending() {
        if (this.sendTask != null) {
            this.sendTask.cancel();
            this.sendTask = null;
        }
    }

    private void sendNextPacket() {
        this.sentChunks.clear();
        this.sendingPack = null;
        ResourcePackDataInfoPacket infoPacket = this.pendingPacks.poll();
        if (infoPacket != null && this.player.isConnected()) {
            this.sendingPack = infoPacket;
            this.player.sendPacket(infoPacket);
        }
    }
}
