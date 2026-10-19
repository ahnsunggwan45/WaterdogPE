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
import dev.waterdog.waterdogpe.network.connection.TransportProfile;
import dev.waterdog.waterdogpe.network.connection.peer.BedrockServerSession;
import dev.waterdog.waterdogpe.packs.types.ResourcePack;
import it.unimi.dsi.fastutil.longs.LongArraySet;
import it.unimi.dsi.fastutil.longs.LongSet;
import org.cloudburstmc.protocol.bedrock.packet.*;
import dev.waterdog.waterdogpe.event.defaults.PlayerResourcePackApplyEvent;
import dev.waterdog.waterdogpe.packs.PackManager;
import dev.waterdog.waterdogpe.player.ProxiedPlayer;
import dev.waterdog.waterdogpe.scheduler.TaskHandler;
import org.cloudburstmc.protocol.common.PacketSignal;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

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

    // Diagnostics: whether clients really download CDN packs from their URL, and how long the pack
    // stage takes. Grep the log for [PackCDN] and [PackPhase].
    private final long phaseStartNanos = System.nanoTime();
    // id_version of the packs whose ResourcePacksInfoPacket entry carried a CDN URL for this client.
    private final Set<String> cdnPackIds = new HashSet<>();
    // CDN packs this client asked for in-protocol anyway, logged once each.
    private final Set<String> cdnFallbackPacks = new HashSet<>();
    private final AtomicLong chunksSent = new AtomicLong();
    private final AtomicLong chunkBytesSent = new AtomicLong();
    private final AtomicBoolean phaseLogged = new AtomicBoolean();

    public ResourcePacksHandler(ProxiedPlayer player) {
        this(player, null);
    }

    /**
     * @param sentInfo the ResourcePacksInfoPacket sent to this client right before this handler took
     *                 over, used to tell which packs it was offered through a CDN URL.
     */
    public ResourcePacksHandler(ProxiedPlayer player, ResourcePacksInfoPacket sentInfo) {
        super(player);
        if (sentInfo != null) {
            this.collectCdnPacks(sentInfo);
        }
        BedrockServerSession connection = player.getConnection();
        if (connection != null) {
            connection.addDisconnectListener(reason -> this.logPhase("aborted (" + reason + ")"));
        }
    }

    private void collectCdnPacks(ResourcePacksInfoPacket sentInfo) {
        for (ResourcePacksInfoPacket.Entry entry : sentInfo.getResourcePackInfos()) {
            this.collectCdnPack(entry);
        }
        for (ResourcePacksInfoPacket.Entry entry : sentInfo.getBehaviorPackInfos()) {
            this.collectCdnPack(entry);
        }
    }

    private void collectCdnPack(ResourcePacksInfoPacket.Entry entry) {
        if (entry.getCdnUrl() != null && !entry.getCdnUrl().isEmpty()) {
            this.cdnPackIds.add(entry.getPackId() + "_" + entry.getPackVersion());
        }
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
                    this.noteCdnFallback(packIdVer, "data info request");
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
                this.logPhase("completed");
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

        String packIdVer = packet.getPackId() + "_" + packet.getPackVersion();
        ResourcePackDataInfoPacket info = this.offeredPacks.get(packIdVer);
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

        this.noteCdnFallback(packIdVer, "chunk request");
        this.queueChunk(packet);
        return this.cancel();
    }

    /**
     * Logs, once per pack and session, a client fetching a pack in-protocol although it was given a
     * CDN URL for it, i.e. it did not (or could not) download from the CDN.
     */
    private void noteCdnFallback(String packIdVer, String via) {
        if (!this.cdnPackIds.contains(packIdVer) || !this.cdnFallbackPacks.add(packIdVer)) {
            return;
        }
        ResourcePack pack = this.player.getProxy().getPackManager().getPacksByIdVer().get(packIdVer);
        this.player.getLogger().info("[PackCDN] [{}|{}] Fetching CDN pack {} ({}) in-protocol via {} instead of its URL, platform={} transport={}",
                this.player.getAddress(), this.player.getName(), pack == null ? "?" : pack.getPackName(), packIdVer, via,
                this.player.getDevicePlatform(), this.transportName());
    }

    /**
     * Logs one line per session for the pack stage, from sending ResourcePacksInfoPacket until the
     * client reports COMPLETED or disconnects.
     */
    private void logPhase(String outcome) {
        if (!this.phaseLogged.compareAndSet(false, true)) {
            return;
        }
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - this.phaseStartNanos);
        this.player.getLogger().info("[PackPhase] [{}|{}] Pack stage {} after {}ms, platform={} transport={} cdnOffered={} cdnFetchedInProtocol={} packsRequested={} chunks={} chunkBytes={}",
                this.player.getAddress(), this.player.getName(), outcome, elapsedMillis, this.player.getDevicePlatform(), this.transportName(),
                this.cdnPackIds.size(), this.cdnFallbackPacks.size(), this.offeredPacks.size(), this.chunksSent.get(), this.chunkBytesSent.get());
    }

    private String transportName() {
        BedrockServerSession connection = this.player.getConnection();
        if (connection == null || connection.getPeer() == null) {
            return "unknown";
        }
        return connection.getPeer().getTransportProfile() == TransportProfile.NETHERNET ? "NetherNet" : "RakNet";
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
        String packIdVer = request.getPackId() + "_" + request.getPackVersion();
        ResourcePackDataInfoPacket info = this.offeredPacks.get(packIdVer);
        ResourcePackChunkDataPacket response = info == null ? null : packManager.packChunkDataPacket(packIdVer, request, info.getMaxChunkSize());
        if (response == null) {
            this.player.disconnect("Unknown resource pack!");
            this.stopSending();
            return;
        }

        if (response.getData() != null) {
            this.chunkBytesSent.addAndGet(response.getData().readableBytes());
        }
        this.chunksSent.incrementAndGet();
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
