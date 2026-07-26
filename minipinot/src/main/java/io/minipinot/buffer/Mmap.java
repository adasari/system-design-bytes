package io.minipinot.buffer;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

/**
 * Memory-maps a file into a read-only {@link MappedByteBuffer}. This is the crux of why Pinot
 * segments load almost instantly: the single index file is mmap'd and index readers address bytes
 * directly out of the OS page cache, with no deserialization onto the Java heap. Mirrors the role
 * of Pinot's {@code PinotDataBuffer} (mmap variant).
 *
 * <p>The returned buffer stays valid after the channel is closed.
 */
public final class Mmap {
  private Mmap() {
  }

  public static MappedByteBuffer mapReadOnly(File file)
      throws IOException {
    try (FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
      return channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
    }
  }
}
