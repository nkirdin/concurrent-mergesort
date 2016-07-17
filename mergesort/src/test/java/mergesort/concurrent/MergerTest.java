package mergesort.concurrent;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;

import org.junit.Before;
import org.junit.Test;

/**
 * @author Nikolay Kirdin 2016-07-15
 * @version 0.2.1
 */
public class MergerTest {
    
    @Before
    public void setup() {
        Utils.setSortedChunksQueue(new PriorityBlockingQueue<File>(10,
                new FileLengthComparator()));
    }

    @Test
    public void test() throws Exception {

        Queue<File> sortedChunksQueue = Utils.getSortedChunksQueue();
        
        File[] testFiles = new File[] {
                new File(ClassLoader.getSystemResource("MergerTest01.txt")
                        .getPath()),
                new File(ClassLoader.getSystemResource("MergerTest02.txt")
                        .getPath()),
                new File(ClassLoader.getSystemResource("MergerTest03.txt")
                        .getPath()) };
        File[] chunkOfFiles = new File[3];

        Utils.numberOfChunksForMerging.set(chunkOfFiles.length);
        
        for (int i = 0; i < 3; i++) {
            chunkOfFiles[i] = File.createTempFile(testFiles[i].getName(), null,
                    Utils.getTmpDirFile());
            Files.copy(testFiles[i].toPath(), chunkOfFiles[i].toPath(),
                    StandardCopyOption.REPLACE_EXISTING);
            sortedChunksQueue.offer(chunkOfFiles[i]);
        }

        File mergerOutputFile = File.createTempFile("mergerOutputFile", null,
                Utils.getTmpDirFile());
        Utils.setSourceFile(mergerOutputFile);
        Utils.allChunksSorted.set(true);
        Merger merger = new Merger();
        Thread  mergerThread = new Thread(merger);
        mergerThread.start();
        
        merger.getAllChanksMergedSemaphore().acquire();
        
        Utils.allChunksSorted.set(true);
        
        assertEquals(1L, Utils.numberOfChunksForMerging.get());
        assertEquals(1, sortedChunksQueue.size());

        File mergedFile = sortedChunksQueue.poll();

        List<String> sortedList = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(
                new FileReader(mergedFile))) {
            String inputLine = null;
            while ((inputLine = br.readLine()) != null) {
                sortedList.add(inputLine);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        assertArrayEquals(
                new String[] { "00", "01", "02", "03", "04", "05", "06", "07", "08", "09",
                        "10", "11", "12", "13", "14"},
                sortedList.toArray(new String[0]));
        mergedFile.delete();
        mergerOutputFile.delete();
    }
    
}
