import java.io.*;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;

public class Project3 {

    private static final int BLOCK_SIZE = 512;
    private static final byte[] MAGIC = "4348PRJ3".getBytes();
    private static final long NO_BLOCK = 0L;

    private static final int T = 10;
    private static final int MAX_KEYS = 2 * T - 1;
    private static final int MAX_CHILDREN = MAX_KEYS + 1;

    private static final int CACHE_CAPACITY = 3;

    public static void main(String[] args) {
        if (args.length < 2) {
            usage();
            return;
        }

        String cmd = args[0].toLowerCase();
        String filename = args[1];

        try {
            switch (cmd) {

                case "create":
                    create(filename);
                    break;

                case "insert":
                case "search":
                case "load":
                case "print":
                case "extract":
                    System.err.println("Error: command not implemented yet (Stage 2)");
                    break;

                default:
                    System.err.println("Unknown command: " + cmd);
                    usage();
            }

        } catch (IOException e) {
            System.err.println("I/O error: " + e.getMessage());
        }
    }

    private static void usage() {
        System.out.println("Usage:");
        System.out.println("  java Project3 create file.idx");
        System.out.println("  java Project3 insert file.idx <key> <value>");
        System.out.println("  java Project3 search file.idx <key>");
        System.out.println("  java Project3 load file.idx input.csv");
        System.out.println("  java Project3 print file.idx");
        System.out.println("  java Project3 extract file.idx output.csv");
    }

    private static void create(String filename) throws IOException {
        Path p = Paths.get(filename);
        if (Files.exists(p)) {
            System.err.println("Error: file already exists");
            return;
        }

        try (RandomAccessFile raf = new RandomAccessFile(filename, "rw")) {
            ByteBuffer buf = ByteBuffer.allocate(BLOCK_SIZE);
            buf.order(ByteOrder.BIG_ENDIAN);

            buf.put(MAGIC);
            buf.putLong(NO_BLOCK);
            buf.putLong(1L);

            buf.position(BLOCK_SIZE);
            buf.flip();

            raf.getChannel().write(buf, 0);
        }

        System.out.println("Created index file: " + filename);
    }

    static class BTree implements Closeable {

        private final RandomAccessFile raf;
        private final FileChannel chan;

        long rootBlockId;
        long nextBlockId;

        BTree(String filename) throws IOException {
            this.raf = new RandomAccessFile(filename, "rw");
            this.chan = raf.getChannel();
            readHeader();
        }

        private void readHeader() throws IOException {
            ByteBuffer buf = ByteBuffer.allocate(BLOCK_SIZE);
            buf.order(ByteOrder.BIG_ENDIAN);
            chan.read(buf, 0);
            buf.flip();

            byte[] magicBuf = new byte[8];
            buf.get(magicBuf);
            if (!Arrays.equals(magicBuf, MAGIC)) {
                throw new IOException("Not a valid index file (magic mismatch)");
            }

            rootBlockId = buf.getLong();
            nextBlockId = buf.getLong();
        }

        private void writeHeader() throws IOException {
            ByteBuffer buf = ByteBuffer.allocate(BLOCK_SIZE);
            buf.order(ByteOrder.BIG_ENDIAN);

            buf.put(MAGIC);
            buf.putLong(rootBlockId);
            buf.putLong(nextBlockId);

            buf.position(BLOCK_SIZE);
            buf.flip();

            chan.write(buf, 0);
        }

        @Override
        public void close() throws IOException {
            writeHeader();
            chan.close();
            raf.close();
        }
    }
}
