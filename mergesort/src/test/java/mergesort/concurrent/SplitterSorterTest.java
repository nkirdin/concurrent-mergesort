package mergesort.concurrent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;

import org.junit.Before;
import org.junit.Test;

/**
 * @author Nikolay Kirdin 2016-07-15
 * @version 0.2.1
 */
public class SplitterSorterTest {

    @Before
    public void setup() {
        Utils.setChunkFileLength(4096);
        Utils.getPointsForSplittingQueue().clear();
        Utils.setSortedChunksQueue(new PriorityBlockingQueue<File>(10,
                new FileLengthComparator()));
    }

    @Test
    public void splitFileTest() throws InterruptedException {
        int maxChunckSize = 4096;
        Utils.setChunkFileLength(maxChunckSize);

        String testPath = ClassLoader.getSystemResource("SplitterTest_40K.txt")
                .getPath();

        File testFile = new File(testPath);

        Utils.setSourceFile(testFile);
        Queue<Tuple<Long, Long>> pointsForSplittingQueue = Utils
                .getPointsForSplittingQueue();

        pointsForSplittingQueue.add(new Tuple<Long, Long>(0L, 10 * 1021L));
        pointsForSplittingQueue
                .add(new Tuple<Long, Long>(10 * 1021L, 20 * 1021L));
        pointsForSplittingQueue
                .add(new Tuple<Long, Long>(20 * 1021L, 30 * 1021L));
        pointsForSplittingQueue
                .add(new Tuple<Long, Long>(30 * 1021L, testFile.length()));
        Utils.numberOfSplittingIntervals.set(pointsForSplittingQueue.size());
        SplitterSorter splitterSorter = new SplitterSorter();
        Thread splitterSorterThread = new Thread(splitterSorter);
        splitterSorterThread.start();
        splitterSorterThread.join();

        Queue<File> sortedChunksQueue = Utils.getSortedChunksQueue();
        assertEquals(4, sortedChunksQueue.size());
        assertEquals(4L, Utils.numberOfSplittingIntervals.get());
        assertEquals(4L, splitterSorter.getNumberOfSortedChunks());

        while (!sortedChunksQueue.isEmpty()) {
            sortedChunksQueue.poll().delete();
        }
    }

    @Test
    public void findStartOfLineTest() throws IOException {
        int maxChunckSize = 4096;
        Utils.setChunkFileLength(maxChunckSize);

        String testPath = ClassLoader.getSystemResource("SplitterTest_40K.txt")
                .getPath();

        File testFile = new File(testPath);

        Utils.setSourceFile(testFile);

        long position = 0;

        long startOfNextLine = SplitterSorter.findStartOfNextLine(position, testFile);
        assertEquals(65L, startOfNextLine);
        assertEquals(
                "000000000001:456789012345678901234567890123456789012345678901234",
                SplitterSorter.readLine(65L, testFile));
        startOfNextLine = SplitterSorter.findStartOfNextLine(testFile.length(),
                testFile);
        assertEquals(-1L, startOfNextLine);
    }

    @Test
    public void findPointsForSplitting()
            throws IOException, InterruptedException {
        int maxChunckSize = 10 * 1024;
        Utils.setChunkFileLength(maxChunckSize);
        Queue<File> sortedChunksQueue = Utils.getSortedChunksQueue();

        String testPath = ClassLoader.getSystemResource("SplitterTest_40K.txt")
                .getPath();

        File testFile = new File(testPath);

        Utils.setSourceFile(testFile);

        SplitterSorter splitterSorter = new SplitterSorter();

        Utils.numberOfSplittingIntervals.set(SplitterSorter.makePointsForSplitting(
                testFile, Utils.getMaxChunkFileLength(),
                Utils.getPointsForSplittingQueue()));

        assertEquals(5, Utils.getPointsForSplittingQueue().size());

        Utils.numberOfSplittingIntervals
                .set(Utils.getPointsForSplittingQueue().size());
        Thread splitterSorterThread = new Thread(splitterSorter);
        splitterSorterThread.start();
        splitterSorterThread.join();

        assertEquals(5, sortedChunksQueue.size());
        assertEquals(5L, Utils.numberOfSplittingIntervals.get());
        assertEquals(5L, splitterSorter.getNumberOfSortedChunks());

        while (!sortedChunksQueue.isEmpty()) {
            File chunk = sortedChunksQueue.poll();
            BufferedReader br = new BufferedReader(new FileReader(chunk));
            String s = br.readLine();
            int count = Integer.parseInt(s.split(":", 2)[0]);
            count++;
            while ((s = br.readLine()) != null) {
                assertEquals(count++, Integer.parseInt(s.split(":", 2)[0]));
            }
            br.close();
            chunk.delete();
        }

    }

    @Test
    public void findPointsForSplittingOnePiece() throws IOException {

        int maxChunckSize = 50 * 1024;

        String testPath = ClassLoader.getSystemResource("SplitterTest_40K.txt")
                .getPath();

        File testFile = new File(testPath);

        SplitterSorter.makePointsForSplitting(testFile, maxChunckSize,
                Utils.getPointsForSplittingQueue());

        assertEquals(1, Utils.getPointsForSplittingQueue().size());
    }
    
    @Test
    public void withoutSorting() throws IOException, InterruptedException {
        Utils.numberOfChunksForMerging.set(0);
        int maxChunckSize = 100 * 1024;
        Utils.setChunkFileLength(maxChunckSize);
        Utils.setSortedChunksQueue(new PriorityBlockingQueue<File>(10,
                new FileLengthComparator()));

        Queue<File> sortedChunksQueue = Utils.getSortedChunksQueue();

        String testPath = ClassLoader.getSystemResource("SorterTest_10K.txt")
                .getPath();
        
        File testFile = new File(testPath);

        Utils.setSourceFile(testFile);

        File chunkOfFile = File.createTempFile(testFile.getName(), null,
                Utils.getTmpDirFile());
        

        Files.copy(testFile.toPath(), chunkOfFile.toPath(),
                StandardCopyOption.REPLACE_EXISTING);
        
        //unsortedChunksQueue.offer(chunkOfFile);

        Utils.numberOfSplittingIntervals.set(SplitterSorter.makePointsForSplitting(
                chunkOfFile, Utils.getMaxChunkFileLength(),
                Utils.getPointsForSplittingQueue()));

        SplitterSorter splitterSorter = new SplitterSorter();
        Thread thread = new Thread(splitterSorter);
        thread.start();
        thread.join();

        assertEquals(1L, splitterSorter.getNumberOfSortedChunks());
        assertEquals(1L, Utils.numberOfChunksForMerging.get());

        File sortedFile = sortedChunksQueue.poll();

        int lineCounter = 0;
        try (BufferedReader br = new BufferedReader(
                new FileReader(sortedFile))) {
            String inputString = null;
            while ((inputString = br.readLine()) != null) {
                int lineNumber = Integer.parseInt(inputString.split(":", 2)[0]);
                assertEquals(lineCounter++, lineNumber);
            }
            assertTrue(lineCounter > 0);
        }

        chunkOfFile.delete();
        sortedFile.delete();

    }

    @Test
    public void withSorting() throws IOException, InterruptedException {
        Utils.setChunkFileLength(64 * 1024);

        Utils.numberOfChunksForMerging.set(0);
        Utils.setSortedChunksQueue(new PriorityBlockingQueue<File>(10,
                new FileLengthComparator()));

        Queue<File> sortedChunksQueue = Utils.getSortedChunksQueue();

        String testPath = ClassLoader.getSystemResource("SplitterTest_40K.txt")
                .getPath();
        File testFile = new File(testPath);

        Utils.setSourceFile(testFile);

        File chunkOfFile = File.createTempFile(testFile.getName(), null,
                Utils.getTmpDirFile());

        Files.copy(testFile.toPath(), chunkOfFile.toPath(),
                StandardCopyOption.REPLACE_EXISTING);

        Utils.numberOfSplittingIntervals.set(SplitterSorter.makePointsForSplitting(
                chunkOfFile, Utils.getMaxChunkFileLength(),
                Utils.getPointsForSplittingQueue()));

        SplitterSorter splitterSorter = new SplitterSorter();
        Thread thread = new Thread(splitterSorter);
        thread.start();
        thread.join();

        assertEquals(Utils.numberOfSplittingIntervals.get(), splitterSorter.getNumberOfSortedChunks());
        assertEquals(Utils.numberOfSplittingIntervals.get(), Utils.numberOfChunksForMerging.get());

        File sortedFile = sortedChunksQueue.poll();
        int lineCounter = 0;
        try (BufferedReader br = new BufferedReader(
                new FileReader(sortedFile))) {
            String inputString = null;
            while ((inputString = br.readLine()) != null) {
                int lineNumber = Integer.parseInt(inputString.split(":", 2)[0]);
                assertEquals(lineCounter++, lineNumber);
            }
            assertTrue(lineCounter > 0);
        }

        chunkOfFile.delete();
        sortedFile.delete();

    }

}
