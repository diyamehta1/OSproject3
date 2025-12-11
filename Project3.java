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
        if (args.length < 2) { usage(); return; }

        String cmd = args[0].toLowerCase();
        String filename = args[1];

        try {
            switch (cmd) {

                case "create":
                    create(filename);
                    break;

                case "insert":
                    if (args.length != 4) return;
                    long key = Long.parseUnsignedLong(args[2]);
                    long value = Long.parseUnsignedLong(args[3]);
                    try (BTree bt = new BTree(filename)) { bt.insert(key, value); }
                    break;

                case "search":
                    if (args.length != 3) return;
                    long sKey = Long.parseUnsignedLong(args[2]);
                    try (BTree bt = new BTree(filename)) {
                        BTree.Result r = bt.search(sKey);
                        if (r.found) System.out.println(r.key + "," + r.value);
                        else {
                            System.err.println("Error: key not found");
                            System.exit(1);
                        }
                    }
                    break;

                case "print":
                    try (BTree bt = new BTree(filename)) { bt.printAll(); }
                    break;

                case "load":
                    if (args.length != 3) return;
                    try (BTree bt = new BTree(filename)) {
                        loadCsv(bt, args[2]);
                    }
                    break;

                case "extract":
                    if (args.length != 3) return;
                    try (BTree bt = new BTree(filename)) {
                        bt.extractCsv(args[2]);
                    }
                    break;

                default:
                    usage();
            }

        } catch (IOException e) {
            System.err.println(e.getMessage());
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
        if (Files.exists(p)) return;

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
    }

    private static void loadCsv(BTree bt, String csv) throws IOException {
        try (BufferedReader br = Files.newBufferedReader(Paths.get(csv))) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                String[] parts = line.split(",", -1);
                long k = Long.parseUnsignedLong(parts[0].trim());
                long v = Long.parseUnsignedLong(parts[1].trim());
                bt.insert(k, v);
            }
        }
    }

    static class BTree implements Closeable {

        private final RandomAccessFile raf;
        private final FileChannel chan;
        private final NodeCache cache;

        long rootBlockId;
        long nextBlockId;

        BTree(String filename) throws IOException {
            this.raf = new RandomAccessFile(filename, "rw");
            this.chan = raf.getChannel();
            readHeader();
            this.cache = new NodeCache(CACHE_CAPACITY, this);
        }

        private void readHeader() throws IOException {
            ByteBuffer buf = ByteBuffer.allocate(BLOCK_SIZE);
            buf.order(ByteOrder.BIG_ENDIAN);
            chan.read(buf, 0);
            buf.flip();
            byte[] magicBuf = new byte[8];
            buf.get(magicBuf);
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

        private Node allocateNode() throws IOException {
            long id = nextBlockId;
            nextBlockId++;
            writeHeader();
            Node n = new Node(id);
            n.dirty = true;
            cache.put(n);
            return n;
        }

        private Node readNode(long id) throws IOException {
            if (id == NO_BLOCK) return null;

            Node cached = cache.get(id);
            if (cached != null) return cached;

            ByteBuffer buf = ByteBuffer.allocate(BLOCK_SIZE);
            buf.order(ByteOrder.BIG_ENDIAN);
            chan.read(buf, id * BLOCK_SIZE);
            buf.flip();

            Node n = new Node(buf.getLong());
            n.parent = buf.getLong();
            n.numKeys = (int) buf.getLong();
            for (int i = 0; i < MAX_KEYS; i++) n.keys[i] = buf.getLong();
            for (int i = 0; i < MAX_KEYS; i++) n.values[i] = buf.getLong();
            for (int i = 0; i < MAX_CHILDREN; i++) n.children[i] = buf.getLong();

            cache.put(n);
            return n;
        }

        void writeNode(Node n) throws IOException {
            ByteBuffer buf = ByteBuffer.allocate(BLOCK_SIZE);
            buf.order(ByteOrder.BIG_ENDIAN);
            buf.putLong(n.blockId);
            buf.putLong(n.parent);
            buf.putLong(n.numKeys);
            for (int i = 0; i < MAX_KEYS; i++) buf.putLong(n.keys[i]);
            for (int i = 0; i < MAX_KEYS; i++) buf.putLong(n.values[i]);
            for (int i = 0; i < MAX_CHILDREN; i++) buf.putLong(n.children[i]);
            buf.position(BLOCK_SIZE);
            buf.flip();
            chan.write(buf, n.blockId * BLOCK_SIZE);
            n.dirty = false;
        }

        static class Result {
            boolean found;
            long key, value;
            Result(boolean f) { found = f; }
        }

        Result search(long key) throws IOException {
            if (rootBlockId == NO_BLOCK) return new Result(false);
            return searchRec(rootBlockId, key);
        }

        private Result searchRec(long id, long key) throws IOException {
            Node n = readNode(id);
            int i = 0;

            while (i < n.numKeys && Long.compareUnsigned(key, n.keys[i]) > 0) i++;

            if (i < n.numKeys && Long.compareUnsigned(key, n.keys[i]) == 0) {
                Result r = new Result(true);
                r.key = n.keys[i];
                r.value = n.values[i];
                return r;
            }

            if (n.children[i] == NO_BLOCK) return new Result(false);
            return searchRec(n.children[i], key);
        }

        void insert(long key, long value) throws IOException {
            if (rootBlockId == NO_BLOCK) {
                Node r = allocateNode();
                r.numKeys = 1;
                r.keys[0] = key;
                r.values[0] = value;
                rootBlockId = r.blockId;
                writeHeader();
                return;
            }

            Node r = readNode(rootBlockId);
            if (r.numKeys == MAX_KEYS) {
                Node s = allocateNode();
                s.children[0] = r.blockId;
                r.parent = s.blockId;
                rootBlockId = s.blockId;
                writeHeader();
                splitChild(s, 0, r);
                insertNonFull(s, key, value);
            } else {
                insertNonFull(r, key, value);
            }
        }

        private void insertNonFull(Node x, long key, long value) throws IOException {
            int i = x.numKeys - 1;

            if (isLeaf(x)) {
                while (i >= 0 && Long.compareUnsigned(key, x.keys[i]) < 0) {
                    x.keys[i + 1] = x.keys[i];
                    x.values[i + 1] = x.values[i];
                    i--;
                }
                x.keys[i + 1] = key;
                x.values[i + 1] = value;
                x.numKeys++;
                x.dirty = true;
                return;
            }

            while (i >= 0 && Long.compareUnsigned(key, x.keys[i]) < 0) i--;

            int childIndex = i + 1;
            Node child = readNode(x.children[childIndex]);

            if (child.numKeys == MAX_KEYS) {
                splitChild(x, childIndex, child);
                if (Long.compareUnsigned(key, x.keys[childIndex]) > 0) {
                    childIndex++;
                }
            }

            insertNonFull(readNode(x.children[childIndex]), key, value);
        }

        private void splitChild(Node x, int i, Node y) throws IOException {
            Node z = allocateNode();
            z.parent = x.blockId;
            z.numKeys = T - 1;

            for (int j = 0; j < T - 1; j++) {
                z.keys[j] = y.keys[j + T];
                z.values[j] = y.values[j + T];
            }

            for (int j = 0; j < T; j++) {
                z.children[j] = y.children[j + T];
                if (z.children[j] != NO_BLOCK) {
                    Node c = readNode(z.children[j]);
                    c.parent = z.blockId;
                    c.dirty = true;
                }
            }

            y.numKeys = T - 1;

            for (int j = x.numKeys; j >= i + 1; j--) {
                x.children[j + 1] = x.children[j];
            }
            x.children[i + 1] = z.blockId;

            for (int j = x.numKeys - 1; j >= i; j--) {
                x.keys[j + 1] = x.keys[j];
                x.values[j + 1] = x.values[j];
            }

            x.keys[i] = y.keys[T - 1];
            x.values[i] = y.values[T - 1];
            x.numKeys++;

            x.dirty = true;
            y.dirty = true;
            z.dirty = true;
        }

        private boolean isLeaf(Node n) {
            for (int i = 0; i < MAX_CHILDREN; i++) {
                if (n.children[i] != NO_BLOCK) return false;
            }
            return true;
        }

        void printAll() throws IOException {
            if (rootBlockId == NO_BLOCK) return;
            printRec(rootBlockId);
        }

        private void printRec(long id) throws IOException {
            Node n = readNode(id);

            for (int i = 0; i < n.numKeys; i++) {
                if (n.children[i] != NO_BLOCK) printRec(n.children[i]);
                System.out.println(Long.toUnsignedString(n.keys[i]) + "," +
                                   Long.toUnsignedString(n.values[i]));
            }

            if (n.children[n.numKeys] != NO_BLOCK)
                printRec(n.children[n.numKeys]);
        }

        void extractCsv(String out) throws IOException {
            try (PrintWriter pw = new PrintWriter(Files.newBufferedWriter(Paths.get(out)))) {
                if (rootBlockId != NO_BLOCK) extractRec(rootBlockId, pw);
            }
        }

        private void extractRec(long id, PrintWriter pw) throws IOException {
            Node n = readNode(id);

            for (int i = 0; i < n.numKeys; i++) {
                if (n.children[i] != NO_BLOCK) extractRec(n.children[i], pw);
                pw.println(Long.toUnsignedString(n.keys[i]) + "," +
                           Long.toUnsignedString(n.values[i]));
            }

            if (n.children[n.numKeys] != NO_BLOCK)
                extractRec(n.children[n.numKeys], pw);
        }

        @Override
        public void close() throws IOException {
            cache.flushAll();
            writeHeader();
            chan.close();
            raf.close();
        }
    }

    static class Node {
        final long blockId;
        long parent = NO_BLOCK;
        int numKeys = 0;
        final long[] keys = new long[MAX_KEYS];
        final long[] values = new long[MAX_KEYS];
        final long[] children = new long[MAX_CHILDREN];
        boolean dirty = false;

        Node(long id) {
            this.blockId = id;
        }
    }

    static class NodeCache {

        private final int capacity;
        private final LinkedHashMap<Long, Node> map;
        private final BTree btree;

        NodeCache(int cap, BTree bt) {
            this.capacity = cap;
            this.btree = bt;

            this.map = new LinkedHashMap<Long, Node>(cap, 0.75f, true) {
                protected boolean removeEldestEntry(Map.Entry<Long, Node> e) {
                    if (size() > capacity) {
                        try {
                            if (e.getValue().dirty) btree.writeNode(e.getValue());
                        } catch (IOException ex) {
                            throw new UncheckedIOException(ex);
                        }
                        return true;
                    }
                    return false;
                }
            };
        }

        Node get(long id) {
            return map.get(id);
        }

        void put(Node n) {
            map.put(n.blockId, n);
        }

        void flushAll() throws IOException {
            for (Node n : map.values()) {
                if (n.dirty) btree.writeNode(n);
            }
            map.clear();
        }
    }
}
