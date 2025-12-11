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
                    System.err.println("Error: command not implemented yet (Stage 3)");
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
