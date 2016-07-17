package mergesort.concurrent;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;

import org.junit.Before;
import org.junit.Test;

/**
 * @author Nikolay Kirdin 2016-07-15
 * @version 0.2.1
 */
public class SplitterTest {

    @Before
    public void setup() {
        Utils.setChunkFileLength(0);
        Utils.getPointsForSplittingQueue().clear();
        Utils.setSortedChunksQueue(new PriorityBlockingQueue<File>(10,
                new FileLengthComparator()));
        Utils.getUnsortedChunksQueue().clear();
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
        Splitter splitter = new Splitter();
        Thread splitterThread = new Thread(splitter);
        splitterThread.start();
        splitterThread.join();

        Queue<File> unSortedChunksQueue = Utils.getUnsortedChunksQueue();
        assertEquals(4, unSortedChunksQueue.size());
        assertEquals(4L, Utils.numberOfSplittingIntervals.get());
        assertEquals(4L, splitter.getNumberOfSplittedChunks());

        while (!unSortedChunksQueue.isEmpty()) {
            unSortedChunksQueue.poll().delete();
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

        long startOfNextLine = Splitter.findStartOfNextLine(position, testFile);
        assertEquals(65L, startOfNextLine);
        assertEquals(
                "000000000001:456789012345678901234567890123456789012345678901234",
                Splitter.readLine(65L, testFile));
        startOfNextLine = Splitter.findStartOfNextLine(testFile.length(),
                testFile);
        assertEquals(-1L, startOfNextLine);
    }

    @Test
    public void findPointsForSplitting()
            throws IOException, InterruptedException {
        int maxChunckSize = 10 * 1024;
        Utils.setChunkFileLength(maxChunckSize);
        Queue<File> unSortedChunksQueue = Utils.getUnsortedChunksQueue();

        String testPath = ClassLoader.getSystemResource("SplitterTest_40K.txt")
                .getPath();

        File testFile = new File(testPath);

        Utils.setSourceFile(testFile);

        Splitter splitter = new Splitter();

        Utils.numberOfSplittingIntervals.set(Splitter.makePointsForSplitting(
                testFile, Utils.getMaxChunkFileLength(),
                Utils.getPointsForSplittingQueue()));

        assertEquals(5, Utils.getPointsForSplittingQueue().size());

        Utils.numberOfSplittingIntervals
                .set(Utils.getPointsForSplittingQueue().size());
        Thread splitterThread = new Thread(splitter);
        splitterThread.start();
        splitterThread.join();

        assertEquals(5, unSortedChunksQueue.size());
        assertEquals(5L, Utils.numberOfSplittingIntervals.get());
        assertEquals(5L, splitter.getNumberOfSplittedChunks());

        while (!unSortedChunksQueue.isEmpty()) {
            File chunk = unSortedChunksQueue.poll();
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

        Splitter.makePointsForSplitting(testFile, maxChunckSize,
                Utils.getPointsForSplittingQueue());

        assertEquals(1, Utils.getPointsForSplittingQueue().size());
    }
}
