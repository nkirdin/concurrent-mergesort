package mergesort.concurrent;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Date;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import mergesort.concurrent.io.LimitedBufferedFilterInputStream;

/**
 * @author Nikolay Kirdin 2016-07-16
 * @version 0.2.2
 */
public class Splitter implements Runnable {

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

    /*
     * Semaphore for indicating that file completely splitted.
     */
    private final Semaphore fileSplittedSemaphore = new Semaphore(0);

    /*
     * Number of chunks after splitting.
     */
    private final AtomicLong numberOfSplittedChunks = new AtomicLong(0);

    public Semaphore getFileSplittedSemaphore() {
        return fileSplittedSemaphore;
    }

    public long getNumberOfSplittedChunks() {
        return numberOfSplittedChunks.get();
    }

    public Set<Thread> getThreadSet() {
        return threadSet;
    }

    public File splitFile(File file, long startPosition, long endPosition)
            throws IOException {

        File chunkOfFile = File.createTempFile(
                "mrgsrt" + "_s_" + splitNumber.getAndIncrement() + "_", null,
                Utils.getTmpDirFile());

        byte[] byteBuffer = new byte[BUFFER_SIZE];

        try (LimitedBufferedFilterInputStream lfis = new LimitedBufferedFilterInputStream(
                new FileInputStream(file));
                FileOutputStream fos = new FileOutputStream(chunkOfFile);) {

            lfis.setPosition(startPosition);
            lfis.setEndOfStreamPosition(endPosition);

            int numOfBytes;

            while ((numOfBytes = lfis.read(byteBuffer, 0,
                    byteBuffer.length)) != -1) {
                fos.write(byteBuffer, 0, numOfBytes);
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
        BlockingQueue<Tuple<Long, Long>> points = Utils
                .getPointsForSplittingQueue();
        Queue<File> chunks = Utils.getUnsortedChunksQueue();
        File file = Utils.getSourceFile();

        while (Utils.numberOfSplittingIntervals.get() != numberOfSplittedChunks
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
                    chunkOfFile = splitFile(file, start, end);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                chunks.offer(chunkOfFile);
                numberOfSplittedChunks.getAndIncrement();
                if (Utils.isVerbose())
                    System.out.println("mergesort: " + new Date()
                            + " : Splitter made chunk: " + start + " " + end
                            + " " + chunkOfFile);
            }
        }

        if (Utils.numberOfSplittingIntervals.get() == numberOfSplittedChunks
                .get() && Utils.sourceFileSplitted.compareAndSet(false,  true)) {
            fileSplittedSemaphore.release();
            if (Utils.isVerbose())
                System.out.println("mergesort: " + new Date()
                        + " : Splitter completely splitted file");
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
