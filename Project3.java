import java.io.*;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;

/**
 * Project3.java
 * Java implementation for CS4348 Project 3 (B-Tree index file).
 *
 * Block size: 512 bytes
 * Header (block 0):
 *   8 bytes: magic "4348PRJ3" (ASCII)
 *   8 bytes: root block id (0 if empty)
 *   8 bytes: next block id
 *   remaining unused
 *
 * Node block:
 *   8 bytes: block id
 *   8 bytes: parent block id (0 if none/root)
 *   8 bytes: num_keys
 *   keys: 19 * 8 bytes = 152
 *   values: 19 * 8 bytes = 152
 *   children: 20 * 8 bytes = 160
 *   unused -> pad to 512
 *
 * B-tree minimal degree t = 10 -> max keys = 19, max children = 20
 *
 * LRU cache of nodes: capacity = 3
 *
 * Commands:
 *   create filename
 *   insert filename key value
 *   search filename key
 *   load filename input.csv
 *   print filename
 *   extract filename output.csv
 */
public class Project3 {

    // Constants
    private static final int BLOCK_SIZE = 512;
    private static final byte[] MAGIC = "4348PRJ3".getBytes(); // exactly 8 bytes
    private static final long NO_BLOCK = 0L;

    // B-tree params
    private static final int T = 10;
    private static final int MAX_KEYS = 2 * T - 1;   // 19
    private static final int MAX_CHILDREN = MAX_KEYS + 1; // 20

    // LRU cache capacity
    private static final int CACHE_CAPACITY = 3;

    // Main
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
                    if (args.length != 4) { System.err.println("insert needs: file key value"); return; }
                    long key = parseUnsignedLong(args[2]);
                    long value = parseUnsignedLong(args[3]);
                    try (BTree bt = new BTree(filename, /*createIfMissing*/ false)) {
                        bt.insert(key, value);
                    }
                    break;
                case "search":
                    if (args.length != 3) { System.err.println("search needs: file key"); return; }
                    long sk = parseUnsignedLong(args[2]);
                    try (BTree bt = new BTree(filename, false)) {
                        BTree.Result r = bt.search(sk);
                        if (r.found) {
                            System.out.println(r.key + "," + r.value);
                        } else {
                            System.err.println("Error: key not found");
                            System.exit(1);
                        }
                    }
                    break;
                case "load":
                    if (args.length != 3) { System.err.println("load needs: file input.csv"); return; }
                    String csvIn = args[2];
                    try (BTree bt = new BTree(filename, false)) {
                        loadCsvAndInsert(bt, csvIn);
                    }
                    break;
                case "print":
                    try (BTree bt = new BTree(filename, false)) {
                        bt.printAll();
                    }
                    break;
                case "extract":
                    if (args.length != 3) { System.err.println("extract needs: file output.csv"); return; }
                    String csvOut = args[2];
                    try (BTree bt = new BTree(filename, false)) {
                        bt.extractCsv(csvOut);
                    }
                    break;
                default:
                    System.err.println("Unknown command: " + cmd);
                    usage();
            }
        } catch (IOException e) {
            System.err.println("I/O error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
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

    private static long parseUnsignedLong(String s) {
        // Java long is signed but we'll treat as unsigned in storage
        return Long.parseUnsignedLong(s);
    }

    private static void create(String filename) throws IOException {
        Path p = Paths.get(filename);
        if (Files.exists(p)) {
            System.err.println("Error: file already exists");
            System.exit(1);
        }
        try (RandomAccessFile raf = new RandomAccessFile(filename, "rw")) {
            // Create header block (512 bytes)
            ByteBuffer buf = ByteBuffer.allocate(BLOCK_SIZE);
            buf.order(ByteOrder.BIG_ENDIAN);
            // magic 8 bytes
            buf.put(MAGIC);
            // root id = 0 (empty)
            buf.putLong(NO_BLOCK);
            // next block id = 1
            buf.putLong(1L);
            // rest unused (zeros)
            buf.position(BLOCK_SIZE);
            buf.flip();
            raf.getChannel().write(buf, 0);
            System.out.println("Created index file: " + filename);
        }
    }

    private static void loadCsvAndInsert(BTree bt, String csvIn) throws IOException {
        Path p = Paths.get(csvIn);
        if (!Files.exists(p)) {
            System.err.println("Error: CSV file does not exist");
            System.exit(1);
        }
        try (BufferedReader br = Files.newBufferedReader(p)) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                String[] parts = line.split(",", -1);
                if (parts.length < 2) {
                    System.err.println("Skipping malformed line: " + line);
                    continue;
                }
                long k = parseUnsignedLong(parts[0].trim());
                long v = parseUnsignedLong(parts[1].trim());
                bt.insert(k, v);
            }
        }
    }

    // ---------- BTree class and node cache ----------
    static class BTree implements Closeable {
        private final RandomAccessFile raf;
        private final FileChannel chan;
        private long rootBlockId;
        private long nextBlockId;
        private final NodeCache cache;

        BTree(String filename, boolean createIfMissing) throws IOException {
            Path path = Paths.get(filename);
            if (!Files.exists(path)) {
                if (createIfMissing) {
                    create(filename);
                } else {
                    throw new FileNotFoundException("Index file not found: " + filename);
                }
            }
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
            if (!Arrays.equals(magicBuf, MAGIC)) {
                throw new IOException("Not a valid index file (magic mismatch)");
            }
            this.rootBlockId = buf.getLong();
            this.nextBlockId = buf.getLong();
            // rest unused
        }

        private void writeHeader() throws IOException {
            ByteBuffer buf = ByteBuffer.allocate(BLOCK_SIZE);
            buf.order(ByteOrder.BIG_ENDIAN);
            buf.put(MAGIC);
            buf.putLong(rootBlockId);
            buf.putLong(nextBlockId);
            // rest zeros
            buf.position(BLOCK_SIZE);
            buf.flip();
            chan.write(buf, 0);
            chan.force(true);
        }

        // allocate new node: assign block id = nextBlockId, increment, write header
        private Node allocateNode() throws IOException {
            long id = nextBlockId;
            nextBlockId++;
            writeHeader();
            Node n = new Node(id);
            n.setDirty(true);
            cache.put(n);
            return n;
        }

        // read node from disk (or from cache)
        private Node readNode(long blockId) throws IOException {
            if (blockId == NO_BLOCK) return null;
            Node n = cache.get(blockId);
            if (n != null) return n;
            // else read from disk
            ByteBuffer buf = ByteBuffer.allocate(BLOCK_SIZE);
            buf.order(ByteOrder.BIG_ENDIAN);
            int read = chan.read(buf, blockId * (long)BLOCK_SIZE);
            if (read != BLOCK_SIZE) {
                throw new IOException("Failed to read full node block at " + blockId);
            }
            buf.flip();
            long bid = buf.getLong();
            long parent = buf.getLong();
            long numKeys = buf.getLong();
            Node node = new Node(bid);
            node.parent = parent;
            node.numKeys = (int)numKeys;
            for (int i = 0; i < MAX_KEYS; i++) {
                node.keys[i] = buf.getLong();
            }
            for (int i = 0; i < MAX_KEYS; i++) {
                node.values[i] = buf.getLong();
            }
            for (int i = 0; i < MAX_CHILDREN; i++) {
                node.children[i] = buf.getLong();
            }
            node.setDirty(false);
            cache.put(node);
            return node;
        }

        // write node to disk (immediate)
        private void writeNode(Node node) throws IOException {
            ByteBuffer buf = ByteBuffer.allocate(BLOCK_SIZE);
            buf.order(ByteOrder.BIG_ENDIAN);
            buf.putLong(node.blockId);
            buf.putLong(node.parent);
            buf.putLong(node.numKeys);
            for (int i = 0; i < MAX_KEYS; i++) buf.putLong(node.keys[i]);
            for (int i = 0; i < MAX_KEYS; i++) buf.putLong(node.values[i]);
            for (int i = 0; i < MAX_CHILDREN; i++) buf.putLong(node.children[i]);
            // pad remainder zeros implicitly by allocating fixed size and using put operations
            buf.position(BLOCK_SIZE);
            buf.flip();
            chan.write(buf, node.blockId * (long)BLOCK_SIZE);
            chan.force(true);
            node.setDirty(false);
        }

        // Exposed for NodeCache eviction
        void flushNode(Node n) throws IOException {
            if (n == null) return;
            if (n.isDirty()) writeNode(n);
        }

        // Search public
        static class Result {
            boolean found;
            long key;
            long value;
            long nodeId;
            int index;
            Result(boolean f) { found = f; }
        }

        Result search(long key) throws IOException {
            if (rootBlockId == NO_BLOCK) return new Result(false);
            return searchRecursive(rootBlockId, key);
        }

        private Result searchRecursive(long blockId, long key) throws IOException {
            Node n = readNode(blockId);
            int i = 0;
            while (i < n.numKeys && Long.compareUnsigned(key, n.keys[i]) > 0) i++;
            if (i < n.numKeys && Long.compareUnsigned(key, n.keys[i]) == 0) {
                Result r = new Result(true);
                r.key = n.keys[i];
                r.value = n.values[i];
                r.nodeId = n.blockId;
                r.index = i;
                return r;
            } else {
                long child = n.children[i];
                if (child == NO_BLOCK) return new Result(false);
                return searchRecursive(child, key);
            }
        }

        // Insert public
        void insert(long key, long value) throws IOException {
            if (rootBlockId == NO_BLOCK) {
                // create root node
                Node root = allocateNode();
                root.parent = NO_BLOCK;
                root.numKeys = 1;
                root.keys[0] = key;
                root.values[0] = value;
                // all children zero by default
                root.setDirty(true);
                rootBlockId = root.blockId;
                writeHeader();
                flushNode(root); // ensure root written
                return;
            }
            Node root = readNode(rootBlockId);
            if (root.numKeys == MAX_KEYS) {
                // split root
                Node s = allocateNode();
                s.parent = NO_BLOCK;
                s.numKeys = 0;
                s.children[0] = root.blockId;
                root.parent = s.blockId;
                s.setDirty(true);
                root.setDirty(true);
                // update root id
                rootBlockId = s.blockId;
                writeHeader();
                splitChild(s, 0, root);
                insertNonFull(s, key, value);
            } else {
                insertNonFull(root, key, value);
            }
        }

        // split child y of parent x at index i
        private void splitChild(Node x, int i, Node y) throws IOException {
            // y is full (MAX_KEYS). Create z
            Node z = allocateNode();
            z.parent = x.blockId;
            // z will take T-1 keys from y (i.e., keys T..MAX_KEYS-1 -> move)
            z.numKeys = T - 1;
            // copy keys and values
            for (int j = 0; j < T - 1; j++) {
                z.keys[j] = y.keys[j + T];
                z.values[j] = y.values[j + T];
            }
            // copy children if any
            for (int j = 0; j < T; j++) {
                z.children[j] = y.children[j + T];
                if (z.children[j] != NO_BLOCK) {
                    try {
                        Node child = readNode(z.children[j]);
                        child.parent = z.blockId;
                        child.setDirty(true);
                    } catch (IOException ex) {
                        throw new IOException("Failed updating child parent on split: " + ex.getMessage());
                    }
                }
            }
            y.numKeys = T - 1;

            // shift x's children to make room
            for (int j = x.numKeys; j >= i+1; j--) {
                x.children[j+1] = x.children[j];
            }
            x.children[i+1] = z.blockId;

            // shift keys/values in x to make room for median
            for (int j = x.numKeys - 1; j >= i; j--) {
                x.keys[j+1] = x.keys[j];
                x.values[j+1] = x.values[j];
            }

            // median key = y.keys[T-1] moves up to x
            x.keys[i] = y.keys[T-1];
            x.values[i] = y.values[T-1];
            x.numKeys = x.numKeys + 1;

            x.setDirty(true);
            y.setDirty(true);
            z.setDirty(true);
        }

        // insert into node known to be non-full
        private void insertNonFull(Node x, long key, long value) throws IOException {
            int i = x.numKeys - 1;
            if (isLeaf(x)) {
                // shift to make space
                while (i >= 0 && Long.compareUnsigned(key, x.keys[i]) < 0) {
                    x.keys[i+1] = x.keys[i];
                    x.values[i+1] = x.values[i];
                    i--;
                }
                // Insert at i+1 (if equal keys allowed? spec doesn't forbid duplicates — we'll allow duplicates and insert after existing)
                int pos = i+1;
                x.keys[pos] = key;
                x.values[pos] = value;
                x.numKeys++;
                x.setDirty(true);
                return;
            } else {
                // find child index
                while (i >= 0 && Long.compareUnsigned(key, x.keys[i]) < 0) i--;
                int childIndex = i+1;
                long childId = x.children[childIndex];
                Node child = readNode(childId);
                if (child.numKeys == MAX_KEYS) {
                    splitChild(x, childIndex, child);
                    // after split, the middle key moves up to x.keys[childIndex]
                    if (Long.compareUnsigned(key, x.keys[childIndex]) > 0) {
                        childIndex = childIndex + 1;
                    }
                }
                Node nextChild = readNode(x.children[childIndex]);
                insertNonFull(nextChild, key, value);
            }
        }

        private boolean isLeaf(Node n) {
            for (int i = 0; i < MAX_CHILDREN; i++) {
                if (n.children[i] != NO_BLOCK) return false;
            }
            return true;
        }

        // print inorder traversal
        void printAll() throws IOException {
            if (rootBlockId == NO_BLOCK) return;
            try (PrintWriter out = new PrintWriter(new OutputStreamWriter(System.out))) {
                traverseAndOutput(rootBlockId, out);
                out.flush();
            }
        }

        private void traverseAndOutput(long blockId, PrintWriter out) throws IOException {
            Node n = readNode(blockId);
            for (int i = 0; i < n.numKeys; i++) {
                long leftChild = n.children[i];
                if (leftChild != NO_BLOCK) traverseAndOutput(leftChild, out);
                out.println(Long.toUnsignedString(n.keys[i]) + "," + Long.toUnsignedString(n.values[i]));
            }
            long right = n.children[n.numKeys];
            if (right != NO_BLOCK) traverseAndOutput(right, out);
        }

        void extractCsv(String outFilename) throws IOException {
            Path p = Paths.get(outFilename);
            if (Files.exists(p)) {
                System.err.println("Error: output file already exists");
                System.exit(1);
            }
            try (BufferedWriter bw = Files.newBufferedWriter(p);
                 PrintWriter pw = new PrintWriter(bw)) {
                if (rootBlockId != NO_BLOCK) traverseAndWriteCsv(rootBlockId, pw);
            }
        }

        private void traverseAndWriteCsv(long blockId, PrintWriter pw) throws IOException {
            Node n = readNode(blockId);
            for (int i = 0; i < n.numKeys; i++) {
                long lc = n.children[i];
                if (lc != NO_BLOCK) traverseAndWriteCsv(lc, pw);
                pw.println(Long.toUnsignedString(n.keys[i]) + "," + Long.toUnsignedString(n.values[i]));
            }
            long right = n.children[n.numKeys];
            if (right != NO_BLOCK) traverseAndWriteCsv(right, pw);
        }

        // Closeable: flush cache then channel
        @Override
        public void close() throws IOException {
            // flush cached nodes
            cache.flushAll();
            // write header (in case root/next changed)
            writeHeader();
            chan.close();
            raf.close();
        }
    }

    // Node representation in-memory
    static class Node {
        final long blockId;
        long parent = NO_BLOCK;
        int numKeys = 0;
        final long[] keys = new long[MAX_KEYS];
        final long[] values = new long[MAX_KEYS];
        final long[] children = new long[MAX_CHILDREN];
        private boolean dirty = false;

        Node(long blockId) {
            this.blockId = blockId;
            Arrays.fill(keys, 0L);
            Arrays.fill(values, 0L);
            Arrays.fill(children, NO_BLOCK);
        }

        boolean isDirty() { return dirty; }
        void setDirty(boolean d) { dirty = d; }
        @Override
        public String toString() {
            return String.format("Node{id=%d parent=%d nKeys=%d}", blockId, parent, numKeys);
        }
    }

    // Small LRU cache for nodes: when evicting, flush node to disk (if dirty).
    static class NodeCache {
        private final int capacity;
        private final LinkedHashMap<Long, Node> map;
        private final BTree btree;

        NodeCache(int capacity, BTree btree) {
            this.capacity = capacity;
            this.btree = btree;
            this.map = new LinkedHashMap<Long, Node>(capacity, 0.75f, true) {
                protected boolean removeEldestEntry(Map.Entry<Long, Node> eldest) {
                    if (size() > NodeCache.this.capacity) {
                        Node ev = eldest.getValue();
                        try {
                            // flush evicted node to disk if dirty
                            btree.flushNode(ev);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                        return true;
                    } else return false;
                }
            };
        }

        synchronized Node get(long blockId) {
            return map.get(blockId);
        }

        // put node into cache; if exists replace
        synchronized void put(Node n) {
            map.put(n.blockId, n);
        }

        synchronized void flushAll() throws IOException {
            for (Node n : map.values()) {
                if (n.isDirty()) btree.flushNode(n);
            }
            // also clear map to avoid holding onto nodes after close
            map.clear();
        }
    }
}
