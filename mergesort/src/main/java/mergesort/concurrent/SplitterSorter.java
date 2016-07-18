package mergesort.concurrent;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import mergesort.concurrent.io.LimitedBufferedFilterInputStream;

/**
 * @author Nikolay Kirdin 2016-07-17
 * @version 0.3
 */
public class SplitterSorter implements Runnable {

    public static final int SUPPOSED_AVERAGE_STRING_LENGTH_IN_BYTES = 80;

    /*
     * Number of sorted chunks.
     */
    private final AtomicInteger numberOfSortedChunks = new AtomicInteger(0);

    /*
     * Semaphore for indicating that all chunks completely sorted.
     */
    private final Semaphore allChanksSortedSemaphore = new Semaphore(0);

    public Semaphore getAllChanksSortedSemaphore() {
        return allChanksSortedSemaphore;
    }

    public int getNumberOfSortedChunks() {
        return numberOfSortedChunks.get();
    }

    /*
     * Byte buffer size in bytes.
     */
    public static final int BUFFER_SIZE = 16 * 1024;

    /*
     * Number of splitted chunk.
     */
    private final AtomicInteger splitNumber = new AtomicInteger(0);

    /*
     * Set with threads for controlling state and health of threads.
     */
    private final Set<Thread> threadSet = ConcurrentHashMap
            .<Thread> newKeySet();

    public Set<Thread> getThreadSet() {
        return threadSet;
    }

    public File splitAndSortFile(File file, long startPosition,
            long endPosition) throws IOException {

        Runtime rt = Runtime.getRuntime();

        List<String> strings = new ArrayList<>(
                (int) ((endPosition - startPosition)
                        / SUPPOSED_AVERAGE_STRING_LENGTH_IN_BYTES));

        File chunkOfFile = File.createTempFile(
                "mrgsrt" + "_s_" + splitNumber.getAndIncrement() + "_", null,
                Utils.getTmpDirFile());

        try (LimitedBufferedFilterInputStream lfis = new LimitedBufferedFilterInputStream(
                new FileInputStream(file))) {

            lfis.setPosition(startPosition);
            lfis.setEndOfStreamPosition(endPosition);

            String string;

            while ((string = lfis.readLine()) != null) {
                strings.add(string);
            }
        }
        Collections.sort(strings);

        if (Utils.isVerbose()) {
            long usedMB = (rt.totalMemory() - rt.freeMemory()) / 1024 / 1024;
            System.out.println("mergesort: " + new Date()
                    + " : SplitterSorter. Memory used after sort (MB): "
                    + usedMB);
        }

        try (BufferedWriter bw = new BufferedWriter(
                new FileWriter(chunkOfFile))) {
            for (int i = 0; i < strings.size(); i++) {
                bw.write(strings.get(i));
                bw.newLine();
                strings.set(i, null);
            }
        }

        return chunkOfFile;

    }

    @Override
    public void run() {
        Thread thread = Thread.currentThread();

        synchronized (threadSet) {
            threadSet.add(thread);
        }

        synchronized (this) {
            long freeMemory = Runtime.getRuntime().freeMemory();
            long requestedMemory = Utils.getMaxChunkFileLength()
                    * threadSet.size();

            if (Utils.isVerbose()) {
                System.out.println("mergesort: " + new Date()
                        + " : SplitterSorter. freeMemory (Byte): " + freeMemory
                        + " requested(Byte): " + +requestedMemory);
            }
            if (requestedMemory * 6 > freeMemory) {
                synchronized (threadSet) {
                    if (threadSet.size() > 1) {
                        if (Utils.isVerbose()) {
                            System.out.println("mergesort: " + new Date()
                                    + " : " + thread
                                    + " : SplitterSorter stop execution");
                        }
                        threadSet.remove(thread);
                        return;
                    }
                }
            }
        }

        Semaphore setOfChunksReadySemaphore = Utils
                .getSetofchunksreadysemaphore();

        BlockingQueue<Tuple<Long, Long>> points = Utils
                .getPointsForSplittingQueue();

        Queue<File> chunks = Utils.getSortedChunksQueue();

        File file = Utils.getSourceFile();

        while (Utils.numberOfSplittingIntervals.get() != numberOfSortedChunks
                .get()) {
            Tuple<Long, Long> point = null;

            try {
                point = points.poll(10, TimeUnit.SECONDS);
            } catch (InterruptedException e1) {
            }

            if (point != null) {

                long start = point.getT1();
                long end = point.getT2();

                File chunkOfFile;
                try {
                    chunkOfFile = splitAndSortFile(file, start, end);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                chunks.offer(chunkOfFile);
                Utils.numberOfChunksForMerging.getAndIncrement();
                numberOfSortedChunks.getAndIncrement();

                Utils.updateSetOfChunksSemaphore();

                if (Utils.isVerbose())
                    System.out.println("mergesort: " + new Date()
                            + " : SplitterSorter made and sorted chunk: "
                            + start + " " + end + " " + chunkOfFile);
            }
        }

        if (Utils.numberOfSplittingIntervals.get() == numberOfSortedChunks.get()
                && Utils.allChunksSorted.compareAndSet(false, true)) {
            allChanksSortedSemaphore.release();
            setOfChunksReadySemaphore.release();
            if (Utils.isVerbose())
                System.out.println("mergesort: " + new Date()
                        + " : SplitterSorter completely splitted and sorted all chunks of file");
        }

        synchronized (threadSet) {
            threadSet.remove(thread);
        }

    }

    public static long findStartOfNextLine(long startPosition, File testFile)
            throws IOException {
        if (startPosition >= testFile.length())
            return -1;
        try (LimitedBufferedFilterInputStream lfis = new LimitedBufferedFilterInputStream(
                new FileInputStream(testFile))) {
            lfis.setPosition(startPosition);
            int readedByte;
            while ((readedByte = lfis.read()) != -1 && readedByte != '\n')
                ;
            if (readedByte == -1)
                return -1;
            while ((readedByte = lfis.read()) != -1 && readedByte < ' ')
                ;
            if (readedByte == -1)
                return -1;
            long position = lfis.getEffectiveStreamPosition();
            if (position > 0)
                lfis.setPosition(position - 1);
            return lfis.getEffectiveStreamPosition();
        }
    }

    public static String readLine(long startPosition, File file)
            throws IOException {
        try (LimitedBufferedFilterInputStream lfis = new LimitedBufferedFilterInputStream(
                new FileInputStream(file))) {
            lfis.setPosition(startPosition);
            return lfis.readLine();
        }
    }

    public static int makePointsForSplitting(File sourceFile, long chunkSize,
            Queue<Tuple<Long, Long>> pointsForSplittingQueue) {
        int numOfIntervals = 0;
        long pointStart = 0L;
        long pointEnd = pointStart;
        while (pointEnd < sourceFile.length()) {
            try {
                pointEnd = findStartOfNextLine(pointEnd + chunkSize,
                        sourceFile);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            if (pointEnd == -1)
                break;
            pointsForSplittingQueue
                    .add(new Tuple<Long, Long>(pointStart, pointEnd));
            pointStart = pointEnd;
            numOfIntervals++;
        }
        numOfIntervals++;
        pointsForSplittingQueue
                .add(new Tuple<Long, Long>(pointStart, sourceFile.length()));
        return numOfIntervals;
    }

}
