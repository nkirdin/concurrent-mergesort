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
 * @author Nikolay Kirdin 2016-07-17
 * @version 0.3
 */
public class IntegrationTest {

    @Before
    public void setup() {

    }

    @Test
    public void test() throws Exception {
        Utils.setVerbose(true);
        long startTime = System.currentTimeMillis();
        Utils.setChunkFileLength(4 * 1024);
        Utils.setMaxNumberOfConcurrentThreads(5);
//        String testPath = ClassLoader
//                .getSystemResource("IntegrationTest_500M.txt").getPath();

        // String testPath = ClassLoader
        // .getSystemResource("IntegrationTest_1M.txt").getPath();

         String testPath = ClassLoader
         .getSystemResource("SorterTest_100K.txt").getPath();

        File testFile = new File(testPath);

        Utils.setSourceFile(testFile);

        SplitterSorter splitterSorter = new SplitterSorter();

        int numberOfSplittingIntervals = SplitterSorter.makePointsForSplitting(
                testFile, Utils.getMaxChunkFileLength(),
                Utils.getPointsForSplittingQueue());
        Utils.numberOfSplittingIntervals.set(numberOfSplittingIntervals);

        System.out.println(Utils.getPointsForSplittingQueue());
        System.out.println("Splitting intervals: " + Utils.getPointsForSplittingQueue().size());

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
        int maxMergerThreads = 2;
        Utils.setMaxNumOfMergingChunks(20);

        Utils.setMaxSplitterThreads(maxSplitterThreads); // -p
        Utils.setMaxMergerThreads(maxMergerThreads); // -r

        for (int i = 0; i < maxSplitterThreads; i++) {
            executorService.execute(splitterSorter);
        }

        Merger merger = new Merger();
        for (int i = 0; i < maxMergerThreads; i++) {
            executorService.execute(merger);
        }

        try {
            Utils.checkSemaphoreAndHelth(
                    splitterSorter.getAllChanksSortedSemaphore(),
                    splitterSorter.getThreadSet(), "Splitting");

            if (Utils.isVerbose())
                System.out.println("mergesort: " + new Date()
                        + " : Ack end of splitting and sorting file");

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
                splitterSorter.getNumberOfSortedChunks());
        assertEquals(1L, Utils.numberOfChunksForMerging.get());
        // assertEquals(0, sortedChunksQueue.size());

        int intervals = Utils.numberOfSplittingIntervals.get();
        int chunks = Utils.getMaxNumOfMergingChunks();
        int numberOfMerges = 0;
        if (intervals > 1) {
            int nextRound = intervals;
            do {
                int rounds = (nextRound / chunks);
                numberOfMerges += rounds;
                int mod = nextRound % chunks;
                nextRound = rounds + mod;
            } while (nextRound >= chunks);
            if (nextRound > 1) numberOfMerges++;
        }

        assertEquals(numberOfMerges, Merger.getMergeNumber());

        if (Utils.numberOfSplittingIntervals.get() != splitterSorter
                .getNumberOfSortedChunks()
                || Utils.numberOfChunksForMerging.get() != 1
                || numberOfMerges != Merger.getMergeNumber()
                || mergedFile == null) {
            System.out.println("ERROR: mergesort. Integral internal error.");
            fail("Internal error");
        }

        System.out
                .println("Total Memory: " + Runtime.getRuntime().totalMemory());
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
