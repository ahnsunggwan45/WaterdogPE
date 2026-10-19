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

package dev.waterdog.waterdogpe.packs.types;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class ZipResourcePack extends ResourcePack {

    // Archive tooling metadata which does not count as a pack folder.
    private static final String MACOS_METADATA_FOLDER = "__MACOSX/";

    private final ZipFile zipFile;
    // Folder (with trailing slash) the pack content sits in inside the zip, empty when at the root.
    private final String contentRoot;
    // Taken while loading, not on the network thread when a player first asks for the pack
    private final byte[] hash;
    private ByteBuffer cachedPack;

    public ZipResourcePack(Path file) throws IOException {
        super(file);
        this.hash = sha256(this.packPath);
        try {
            this.zipFile = new ZipFile(this.packPath.toFile());
        } catch (IOException e) {
            throw new IOException("ResourcePack is not zip file!");
        }
        this.contentRoot = findContentRoot(this.zipFile);
    }

    /**
     * Packs are normally zipped with manifest.json at the root. Zips made by compressing the pack
     * folder itself instead hold everything under one top-level folder; accept those too, but only
     * when the root has no manifest and that folder is the only thing in the zip.
     */
    static String findContentRoot(ZipFile zipFile) {
        String manifest = PackManifest.MANIFEST_PATH.toString();
        if (zipFile.getEntry(manifest) != null) {
            return "";
        }

        String folder = null;
        Enumeration<? extends ZipEntry> entries = zipFile.entries();
        while (entries.hasMoreElements()) {
            String name = entries.nextElement().getName();
            if (name.startsWith(MACOS_METADATA_FOLDER)) {
                continue;
            }
            int slash = name.indexOf('/');
            if (slash <= 0) {
                return ""; // A file at the root, so this is not a single wrapped folder.
            }
            String top = name.substring(0, slash + 1);
            if (folder == null) {
                folder = top;
            } else if (!folder.equals(top)) {
                return ""; // More than one top-level folder.
            }
        }
        return folder != null && zipFile.getEntry(folder + manifest) != null ? folder : "";
    }

    public String getContentRoot() {
        return this.contentRoot;
    }

    // Streamed, a pack can be larger than the heap
    private static byte[] sha256(Path path) throws IOException {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
        try (InputStream input = new DigestInputStream(Files.newInputStream(path), digest)) {
            input.transferTo(OutputStream.nullOutputStream());
        }
        return digest.digest();
    }

    public ZipEntry getZipEntry(Path path) {
        // Zip entry names always use forward slashes, whatever the platform separator is.
        StringBuilder name = new StringBuilder(this.contentRoot);
        for (int i = 0; i < path.getNameCount(); i++) {
            if (i > 0) {
                name.append('/');
            }
            name.append(path.getName(i));
        }
        return this.zipFile.getEntry(name.toString());
    }

    @Override
    public InputStream getStream(Path path) throws IOException {
        ZipEntry entry = this.getZipEntry(path);
        if (entry == null) {
            return null;
        }
        return this.zipFile.getInputStream(entry);
    }

    @Override
    public void close() throws IOException {
        zipFile.close();
    }

    @Override
    public void saveToCache() throws IOException {
        try (FileChannel fileChannel = FileChannel.open(this.packPath)) {
            ByteBuffer buffer = ByteBuffer.allocateDirect((int) fileChannel.size());
            fileChannel.read(buffer);
            buffer.rewind();
            this.cachedPack = buffer;
        }
    }

    @Override
    public ByteBuffer getCachedPack() {
        return this.cachedPack == null ? null : this.cachedPack.slice();
    }

    @Override
    public long getPackSize() {
        try {
            return Files.size(this.packPath);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to get size of pack", e);
        }
    }

    @Override
    public byte[] getHash() {
        return this.hash;
    }

    @Override
    public byte[] getChunk(int offset, int length) {
        byte[] chunkData = new byte[(int) Math.min(this.getPackSize() - offset, length)];

        ByteBuffer cachedPack = this.getCachedPack();
        if (cachedPack != null) {
            cachedPack.position(offset);
            cachedPack.get(chunkData, 0, chunkData.length);
            return chunkData;
        }

        // skip and read may stop short, which would send the rest of the chunk as zeros
        try (InputStream inputStream = Files.newInputStream(this.packPath)) {
            inputStream.skipNBytes(offset);
            inputStream.readNBytes(chunkData, 0, chunkData.length);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to read pack chunk", e);
        }
        return chunkData;
    }
}
