package mergesort.concurrent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Date;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

/**
 * @author Nikolay Kirdin 2016-07-16
 * @version 0.2.2
 */
public class IntegrationTest {

    @Before
    public void setup() {

    }

    @Test
    public void test() throws Exception {
        Utils.setVerbose(true);
        long startTime = System.currentTimeMillis();
        Utils.setChunkFileLength(50 * 1024 * 1024);
        Utils.setMaxNumberOfConcurrentThreads(10);
//        String testPath = ClassLoader
//                .getSystemResource("IntegrationTest_500M.txt").getPath();

         String testPath = ClassLoader
         .getSystemResource("SorterTest_100K.txt").getPath();

        File testFile = new File(testPath);

        Utils.setSourceFile(testFile);

        Splitter splitter = new Splitter();

        int numberOfSplittingIntervals = Splitter.makePointsForSplitting(
                testFile, Utils.getMaxChunkFileLength(),
                Utils.getPointsForSplittingQueue());
        Utils.numberOfSplittingIntervals.set(numberOfSplittingIntervals);

        Utils.setSortedChunksQueue(new PriorityBlockingQueue<File>(
                numberOfSplittingIntervals, new FileLengthComparator()));

        MergerSortThreadFactory mergerSortThreadFactory = new MergerSortThreadFactory();
        ExecutorService executorService = Executors.newFixedThreadPool(
                Utils.getMaxNumberOfConcurrentThreads(),
                mergerSortThreadFactory);

        // ThreadPoolExecutor executorService = new ThreadPoolExecutor(9, 9, 5,
        // TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(),
        // mergerSortThreadFactory);
        // executorService.allowCoreThreadTimeOut(true);

        int maxSplitterThreads = 10;
        int maxSorterThreads = 5;
        int maxMergerThreads = 5;
        Utils.setMaxNumOfMergingChunks(12);

        Utils.setMaxSplitterThreads(maxSplitterThreads); // -p
        Utils.setMaxMergerThreads(maxMergerThreads); // -r
        Utils.setMaxSorterThreads(maxSorterThreads); // -s

        for (int i = 0; i < maxSplitterThreads; i++) {
            executorService.execute(splitter);
        }

        Sorter sorter = new Sorter();
        for (int i = 0; i < maxSorterThreads; i++) {
            executorService.execute(sorter);
        }

        Merger merger = new Merger();
        for (int i = 0; i < maxMergerThreads; i++) {
            executorService.execute(merger);
        }

        try {
            Utils.checkSemaphoreAndHelth(splitter.getFileSplittedSemaphore(),
                    splitter.getThreadSet(), "Splitting");

            if (Utils.isVerbose())
                System.out.println("mergesort: " + new Date()
                        + " : Ack end of splitting file");

            Utils.checkSemaphoreAndHelth(sorter.getAllChanksSortedSemaphore(),
                    sorter.getThreadSet(), "Sorting");

            if (Utils.isVerbose())
                System.out.println("mergesort: " + new Date()
                        + " : Ack end of sorting file");

            Utils.checkSemaphoreAndHelth(merger.getAllChanksMergedSemaphore(),
                    merger.getThreadSet(), "Merging");

            if (Utils.isVerbose())
                System.out.println("mergesort: " + new Date()
                        + " : Ack end of merging file");

        } catch (InternalInconsistencyException e) {
            e.printStackTrace();
            System.out.println("ERROR: mergesort. Inconsistency error.");
            // System.exit(254);
            fail();
        }

        BlockingQueue<File> sortedChunksQueue = Utils.getSortedChunksQueue();

        File mergedFile = sortedChunksQueue.poll(1, TimeUnit.SECONDS);

        assertEquals(Utils.numberOfSplittingIntervals.get(),
                splitter.getNumberOfSplittedChunks());
        assertEquals(Utils.numberOfSplittingIntervals.get(),
                sorter.getNumberOfSortedChunks());
        assertEquals(1L, Utils.numberOfChunksForMerging.get());
        // assertEquals(0, sortedChunksQueue.size());

        int intervals = Utils.numberOfSplittingIntervals.get();
        int chunks = Utils.getMaxNumOfMergingChunks();
        int numberOfMerges = 0;
        int mod = 0;
       do {
            int rounds = (intervals / chunks);
            mod = intervals % chunks;
            int nextRound = rounds + mod;
            numberOfMerges += rounds;
            intervals = nextRound;
        } while (intervals > chunks);
       if (Utils.numberOfSplittingIntervals.get() != 1 && mod != 0) numberOfMerges++;

        assertEquals(numberOfMerges, Merger.getMergeNumber());

        if (Utils.numberOfSplittingIntervals.get() != splitter
                .getNumberOfSplittedChunks()
                || Utils.numberOfSplittingIntervals.get() != sorter
                        .getNumberOfSortedChunks()
                || Utils.numberOfChunksForMerging.get() != 1
                || numberOfMerges != Merger.getMergeNumber()
                || mergedFile == null) {
            System.out.println("ERROR: mergesort. Integral internal error.");
            fail("Internal error");
        }
        
        System.out.println(
                "Total Memory: " + Runtime.getRuntime().totalMemory());
        System.out.println(
                "Duration: " + (System.currentTimeMillis() - startTime));
        System.out
                .println("Test: end of sort " + new Date() + " " + mergedFile);

        try (BufferedReader br = new BufferedReader(
                new FileReader(mergedFile))) {
            String inputLine = null;
            int counter = 0;
            while ((inputLine = br.readLine()) != null) {
                String lineNumber = inputLine.split(":", 2)[0];
                assertEquals(counter++, Integer.parseInt(lineNumber));
            }
            assertTrue(counter > 0);
        }

        mergedFile.delete();
        System.out.println("Test: end of check " + new Date());
    }
}
