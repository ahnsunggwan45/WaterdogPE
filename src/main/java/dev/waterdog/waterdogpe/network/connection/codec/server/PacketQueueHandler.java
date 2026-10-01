package dev.waterdog.waterdogpe.network.connection.codec.server;

import dev.waterdog.waterdogpe.network.NetworkMetrics;
import dev.waterdog.waterdogpe.network.connection.codec.batch.BatchFlags;
import dev.waterdog.waterdogpe.network.connection.peer.BedrockServerSession;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.internal.PlatformDependent;
import lombok.extern.log4j.Log4j2;
import org.cloudburstmc.protocol.bedrock.netty.BedrockBatchWrapper;

import java.util.Queue;

@Log4j2
public class PacketQueueHandler extends ChannelDuplexHandler {
    public static final String NAME = "packet-queue-handler";
    public static final int MAX_PACKETS = 100_000;
    // Every batch holds at least one packet, so sizing the queue to the packet limit lets MAX_PACKETS decide.
    // A smaller bound would not help anyway: netty raises any MPSC capacity below 2048 to 2048.
    private static final int MAX_BATCHES = MAX_PACKETS;

    private final BedrockServerSession session;

    private int packetCounter = 0;
    private final Queue<BedrockBatchWrapper> queue = PlatformDependent.newMpscQueue(MAX_BATCHES);

    private volatile boolean finished;
    private volatile boolean dropQueued;

    public PacketQueueHandler(BedrockServerSession session) {
        this.session = session;
    }

    /**
     * Drop queued batches instead of flushing them on removal.
     * Used when the transfer fails and the queue holds packets from the abandoned target server.
     */
    public void dropQueued() {
        this.dropQueued = true;
    }

    private void finish(ChannelHandlerContext ctx, boolean send) {
        if (this.finished) {
            return;
        }
        this.finished = true;

        if (ctx.pipeline().get(NAME) == this) {
            ctx.pipeline().remove(this);
        }

        BedrockBatchWrapper batch;
        while ((batch = this.queue.poll()) != null) {
            if (send) {
                ctx.write(batch);
            } else {
                batch.release();
            }
        }

        if (send) {
            ctx.flush();
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        this.finish(ctx, false);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        this.finish(ctx, !this.dropQueued && ctx.channel().isActive());
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        if (this.finished || !(msg instanceof BedrockBatchWrapper batch) || batch.hasFlag(BatchFlags.SKIP_QUEUE)) {
            ctx.write(msg, promise);
            return;
        }

        if (this.queue.offer(batch) && this.packetCounter < MAX_PACKETS) {
            this.packetCounter += batch.getPackets().size();
        } else {
            log.warn("[{}] has reached maximum transfer queue capacity: batches={} packets={}", this.session.getSocketAddress(), this.queue.size(), this.packetCounter);
            this.finish(ctx, false);
            this.session.disconnect("Transfer queue got too large");

            NetworkMetrics metrics = ctx.channel().attr(NetworkMetrics.ATTRIBUTE).get();
            if (metrics != null) {
                metrics.packetQueueTooLarge();
            }
        }
    }
}
