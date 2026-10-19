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

import dev.waterdog.waterdogpe.network.connection.TransportProfile;
import dev.waterdog.waterdogpe.network.connection.peer.BedrockServerSession;
import dev.waterdog.waterdogpe.packs.types.ResourcePack;
import org.cloudburstmc.protocol.bedrock.packet.*;
import dev.waterdog.waterdogpe.event.defaults.PlayerResourcePackApplyEvent;
import dev.waterdog.waterdogpe.packs.PackManager;
import dev.waterdog.waterdogpe.player.ProxiedPlayer;
import org.cloudburstmc.protocol.common.PacketSignal;

import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Upstream handler handling proxy manager resource packs.
 */
public class ResourcePacksHandler extends AbstractUpstreamHandler {

    // The client buffers out-of-order chunks in memory, so at most this many wait there
    private static final int HOLD_WINDOW = 100;

    // Packs offered to this client by id_version
    private final Map<String, ResourcePackDataInfoPacket> offeredPacks = new HashMap<>();
    private final Map<String, BitSet> sentChunks = new HashMap<>();
    // The client writes chunks in order on one IO thread that also unzips finished packs, and only shows an
    // in-order chunk on its progress bar once written. The first chunk of every window goes last, so the rest
    // skip that queue as on BDS.
    private final Map<String, BitSet> heldChunks = new HashMap<>();

    // Diagnostics: whether clients really download CDN packs from their URL, and how long the pack
    // stage takes. One key=value line each, tagged [PackCDN] and [PackPhase] for log tooling.
    private final long phaseStartNanos = System.nanoTime();
    // CDN URL by id_version, for the packs whose ResourcePacksInfoPacket entry carried one for this client.
    private final Map<String, String> cdnUrls = new HashMap<>();
    // CDN packs this client asked for in-protocol anyway, logged once each.
    private final Set<String> cdnFallbackPacks = new HashSet<>();
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
            sentInfo.getResourcePackInfos().forEach(this::collectCdnUrl);
            sentInfo.getBehaviorPackInfos().forEach(this::collectCdnUrl);
        }
        BedrockServerSession connection = player.getConnection();
        if (connection != null) {
            connection.addDisconnectListener(reason -> this.logPhase("disconnect", reason));
        }
    }

    private void collectCdnUrl(ResourcePacksInfoPacket.Entry entry) {
        if (entry.getCdnUrl() != null && !entry.getCdnUrl().isEmpty()) {
            this.cdnUrls.put(entry.getPackId() + "_" + entry.getPackVersion(), entry.getCdnUrl());
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
                // All up front, as the client's progress bar only counts packs it has been offered
                for (String packIdVer : packet.getPackIds()) {
                    ResourcePackDataInfoPacket response = packManager.packInfoFromIdVer(packIdVer);
                    if (response == null) {
                        this.player.disconnect("disconnectionScreen.resourcePack");
                        break;
                    }
                    this.noteCdnFallback(packIdVer, "data_info");
                    if (this.offeredPacks.putIfAbsent(packIdVer, response) == null) {
                        this.player.sendPacket(response);
                    }
                }
                break;
            case HAVE_ALL_PACKS:
                PlayerResourcePackApplyEvent event = new PlayerResourcePackApplyEvent(this.player, packManager.getStackPacket());
                this.player.getProxy().getEventManager().callEvent(event);
                this.player.getConnection().sendPacket(event.getStackPacket());
                break;
            case COMPLETED:
                this.logPhase("done", null);
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
        int index = packet.getChunkIndex();
        if (info == null || index < 0 || index >= info.getChunkCount()) {
            return this.cancel();
        }

        this.noteCdnFallback(packIdVer, "chunk_request");
        // Each chunk is sent once
        BitSet sent = this.sentChunks.computeIfAbsent(packIdVer, id -> new BitSet());
        BitSet held = this.heldChunks.computeIfAbsent(packIdVer, id -> new BitSet());
        if (sent.get(index) || held.get(index)) {
            return this.cancel();
        }
        int start = index - index % HOLD_WINDOW;
        int end = (int) Math.min(start + HOLD_WINDOW, info.getChunkCount());
        if (index == start && sent.nextClearBit(start + 1) < end) {
            held.set(index);
            return this.cancel();
        }

        if (this.sendChunk(packIdVer, sent, packet) && held.get(start) && sent.nextClearBit(start + 1) >= end) {
            held.clear(start);
            ResourcePackChunkRequestPacket first = new ResourcePackChunkRequestPacket();
            first.setPackId(packet.getPackId());
            first.setPackVersion(packet.getPackVersion());
            first.setChunkIndex(start);
            this.sendChunk(packIdVer, sent, first);
        }
        return this.cancel();
    }

    private boolean sendChunk(String packIdVer, BitSet sent, ResourcePackChunkRequestPacket request) {
        ResourcePackChunkDataPacket response = this.player.getProxy().getPackManager().packChunkDataPacket(packIdVer, request);
        if (response == null) {
            this.player.disconnect("Unknown resource pack!");
            return false;
        }
        sent.set(request.getChunkIndex());
        if (response.getData() != null) {
            this.chunkBytesSent.addAndGet(response.getData().readableBytes());
        }
        // A batch of its own, as the client asks for many chunks at once and queued ones would share a batch
        this.player.sendPacketImmediately(response);
        return true;
    }

    /**
     * Logs, once per pack and session, a client fetching a pack in-protocol although it was given a
     * CDN URL for it, i.e. it did not (or could not) download from the CDN.
     */
    private void noteCdnFallback(String packIdVer, String via) {
        String url = this.cdnUrls.get(packIdVer);
        if (url == null || !this.cdnFallbackPacks.add(packIdVer)) {
            return;
        }
        ResourcePack pack = this.player.getProxy().getPackManager().getPacksByIdVer().get(packIdVer);
        this.player.getLogger().info("[PackCDN] player=" + value(this.player.getName())
                + " pack=" + packIdVer.substring(0, packIdVer.indexOf('_'))
                + " name=" + value(pack == null ? "?" : pack.getPackName())
                + " url=" + value(url)
                + " event=chunk_fallback via=" + via
                + " platform=" + this.player.getDevicePlatform()
                + " transport=" + this.transportName());
    }

    /**
     * Logs one line per session for the pack stage, from sending ResourcePacksInfoPacket until the
     * client reports COMPLETED or disconnects.
     */
    private void logPhase(String result, CharSequence reason) {
        if (!this.phaseLogged.compareAndSet(false, true)) {
            return;
        }
        int chunkPacks = 0;
        long chunks = 0;
        for (BitSet sent : this.sentChunks.values()) {
            if (!sent.isEmpty()) {
                chunkPacks++;
                chunks += sent.cardinality();
            }
        }
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - this.phaseStartNanos);
        this.player.getLogger().info("[PackPhase] player=" + value(this.player.getName())
                + " platform=" + this.player.getDevicePlatform()
                + " transport=" + this.transportName()
                + " result=" + result
                + " ms=" + elapsedMillis
                + " cdn=" + this.cdnUrls.size()
                + " chunk=" + chunkPacks
                + " cdnChunk=" + this.cdnFallbackPacks.size()
                + " requested=" + this.offeredPacks.size()
                + " chunks=" + chunks
                + " chunkBytes=" + this.chunkBytesSent.get()
                + (reason == null ? "" : " reason=" + value(reason.toString())));
    }

    private String transportName() {
        BedrockServerSession connection = this.player.getConnection();
        if (connection == null || connection.getPeer() == null) {
            return "unknown";
        }
        return connection.getPeer().getTransportProfile() == TransportProfile.NETHERNET ? "NetherNet" : "RakNet";
    }

    // Quotes values containing whitespace (gamertags may) so each key=value stays one token.
    static String value(String value) {
        if (value == null) {
            return "null";
        }
        if (value.isEmpty() || value.chars().anyMatch(c -> Character.isWhitespace(c) || c == '"')) {
            return '"' + value.replace('"', '\'') + '"';
        }
        return value;
    }
}
