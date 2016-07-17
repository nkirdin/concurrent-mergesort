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
 * @author Nikolay Kirdin 2016-07-16
 * @version 0.2.2
 */
public class SorterTest {

    @Before
    public void setup() {
        Utils.numberOfChunksForMerging.set(0);
        Utils.setSortedChunksQueue(new PriorityBlockingQueue<File>(10,
                new FileLengthComparator()));
        Utils.getUnsortedChunksQueue().clear();
    }

    @Test
    public void withoutSorting() throws IOException, InterruptedException {

        Queue<File> sortedChunksQueue = Utils.getSortedChunksQueue();

        String testPath = ClassLoader.getSystemResource("SorterTest_10K.txt")
                .getPath();
        File testFile = new File(testPath);

        Utils.setSourceFile(testFile);

        File chunkOfFile = File.createTempFile(testFile.getName(), null,
                Utils.getTmpDirFile());

        Utils.numberOfSplittingIntervals.set(1);
        Files.copy(testFile.toPath(), chunkOfFile.toPath(),
                StandardCopyOption.REPLACE_EXISTING);
        Queue<File> unsortedChunksQueue = Utils.getUnsortedChunksQueue();
        unsortedChunksQueue.offer(chunkOfFile);
        Sorter sorter = new Sorter();
        Thread thread = new Thread(sorter);
        thread.start();
        thread.join();

        assertEquals(1L, sorter.getNumberOfSortedChunks());
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

        Queue<File> sortedChunksQueue = Utils.getSortedChunksQueue();

        String testPath = ClassLoader.getSystemResource("SplitterTest_40K.txt")
                .getPath();
        File testFile = new File(testPath);

        Utils.setSourceFile(testFile);

        File chunkOfFile = File.createTempFile(testFile.getName(), null,
                Utils.getTmpDirFile());

        Utils.numberOfSplittingIntervals.set(1);
        Files.copy(testFile.toPath(), chunkOfFile.toPath(),
                StandardCopyOption.REPLACE_EXISTING);
        Queue<File> unsortedChunksQueue = Utils.getUnsortedChunksQueue();
        unsortedChunksQueue.offer(chunkOfFile);
        Sorter sorter = new Sorter();
        Thread thread = new Thread(sorter);
        thread.start();
        thread.join();

        assertEquals(1L, sorter.getNumberOfSortedChunks());
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

}
