package mergesort.concurrent;

import java.io.File;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Nikolay Kirdin 2016-07-17
 * @version 0.3
 */
public final class Utils {

    public static final String VERSION = "0.3.0";

    /*
     * Key for output additional information while working.
     */
    private static boolean verbose = false;
    
    /*
     * Semaphore for indicating that set of chunks for merging ready.
     */
    private static final Semaphore setOfChunksReadySemaphore = new Semaphore(0);

    /*
     * Queue with tuples which contain start and end positions of chunks in
     * source file
     */
    private static final BlockingQueue<Tuple<Long, Long>> pointsForSplittingQueue = new LinkedBlockingQueue<>();

    /*
     * Queue with sorted chunks. It is produced by sorter and consumes by
     * merger. At the end of work there should be only one sorted file.
     */
    private static BlockingQueue<File> sortedChunksQueue = new PriorityBlockingQueue<>(
            11, new FileLengthComparator());

    /*
     * Indicates that all chunks were sorted.
     */
    public static final AtomicBoolean allChunksSorted = new AtomicBoolean(
            false);

    /*
     * Indicates that all chunks were merged.
     */
    public static final AtomicBoolean allChunksMerged = new AtomicBoolean(
            false);

    /*
     * Number of splitting intervals of source file.
     */
    public static final AtomicInteger numberOfSplittingIntervals = new AtomicInteger();
    
    public static final AtomicInteger setOfChunks = new AtomicInteger(0);

    /*
     * Number of chunks while sorting and merging.
     */
    public static final AtomicLong numberOfChunksForMerging = new AtomicLong(0);
        
    /*
     * Source file.
     */
    private static File sourceFile;

    /*
     * Directory for temporary files.
     */
    private static File tmpDirFile;

    /*
     * Maximum chunk file length.
     */
    private static long maxChunkFileLength = 50 * 1024 * 1024;

    private static int maxNumOfMergingChunks = 20;

    private static int maxSplitterThreads = 5;

    private static int maxMergerThreads = 1;

    private static int maxNumberOfConcurrentThreads = 5;

    public static int getMaxSplitterThreads() {
        return maxSplitterThreads;
    }

    public static int getMaxMergerThreads() {
        return maxMergerThreads;
    }

    public static void setMaxMergerThreads(int maxMergerThreads) {
        Utils.maxMergerThreads = maxMergerThreads;
    }

    public static void setMaxSplitterThreads(int maxSplitterThreads) {
        Utils.maxSplitterThreads = maxSplitterThreads;
    }

    public static int getMaxNumOfMergingChunks() {
        return maxNumOfMergingChunks;
    }

    public static void setMaxNumOfMergingChunks(int maxNumOfMergingChunks) {
        Utils.maxNumOfMergingChunks = maxNumOfMergingChunks;
    }

    /*
     * Queue of sorted chunks (produced by sorter)
     */
    public static BlockingQueue<File> getSortedChunksQueue() {
        return sortedChunksQueue;
    }

    public static void setSortedChunksQueue(
            BlockingQueue<File> sortedChunksQueue) {
        Utils.sortedChunksQueue = sortedChunksQueue;
    }

    public static File getSourceFile() {
        return sourceFile;
    }

    public static void setSourceFile(File sourceFile) {
        Utils.sourceFile = sourceFile;
    }

    public static long getMaxChunkFileLength() {
        return maxChunkFileLength;
    }

    public static void setChunkFileLength(long maxChunkFileLength) {
        Utils.maxChunkFileLength = maxChunkFileLength;
    }

    public static BlockingQueue<Tuple<Long, Long>> getPointsForSplittingQueue() {
        return pointsForSplittingQueue;
    }

    public static void setTmpDirFile(File tmpDirFile) {
        Utils.tmpDirFile = tmpDirFile;
    }

    public static File getTmpDirFile() {
        return tmpDirFile;
    }

    public static int getMaxNumberOfConcurrentThreads() {
        return maxNumberOfConcurrentThreads;
    }

    public static void setMaxNumberOfConcurrentThreads(
            int maxNumberOfConcurrentThreads) {
        Utils.maxNumberOfConcurrentThreads = maxNumberOfConcurrentThreads;
    }

    public static boolean isVerbose() {
        return verbose;
    }

    public static void setVerbose(boolean verbose) {
        Utils.verbose = verbose;
    }
    
    public static void checkSemaphoreAndHelth(Semaphore semaphore,
            Set<Thread> threadSet, String name)
            throws InternalInconsistencyException {
        boolean acquired = false;
        while (!acquired) {
            try {
                acquired = semaphore.tryAcquire(120, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
            }
            synchronized (threadSet) {
                for (Thread thread : threadSet) {

                    if (!thread.isAlive()) {
                        System.out.println("ERROR: mergesort. " + name
                                + " Internal error.");
                        throw new InternalInconsistencyException(
                                "ERROR: mergesort. " + name
                                        + " Internal error.");
                    }
                }
            }
        }
    }

    public static Semaphore getSetofchunksreadysemaphore() {
        return setOfChunksReadySemaphore;
    }
    
    public static void updateSetOfChunksSemaphore() {
        synchronized(setOfChunks) {
            setOfChunks.getAndIncrement();
            if (setOfChunks.compareAndSet(Utils.getMaxNumOfMergingChunks(), 0)) {
                setOfChunksReadySemaphore.release();
            }
        }
    }

}
