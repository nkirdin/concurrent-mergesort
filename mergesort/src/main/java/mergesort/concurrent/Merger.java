package mergesort.concurrent;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Nikolay Kirdin 2016-07-17
 * @version 0.3
 */

public class Merger implements Runnable {

    private static final AtomicInteger mergeNumber = new AtomicInteger(0);

    private static final AtomicInteger numberOfMergingChunks = new AtomicInteger(
            0);
    
    /*
     * Set with threads for controlling state and health of threads.
     */
    private final Set<Thread> threadSet = ConcurrentHashMap
            .<Thread> newKeySet();

    /*
     * Semaphore for indicating that all chunks merged.
     */
    private final Semaphore allChanksMergedSemaphore = new Semaphore(0);

    public Semaphore getAllChanksMergedSemaphore() {
        return allChanksMergedSemaphore;
    }

    public Set<Thread> getThreadSet() {
        return threadSet;
    }

    public static int getMergeNumber() {
        return mergeNumber.get();
    }

    public void merge(List<File> mergingChunks, File mergedChunkOfFile)
            throws IOException {

        Map<BufferedReader, File> brMap = new HashMap<>();

        Queue<Tuple<String, BufferedReader>> priorityQueue = new PriorityQueue<>();

        try (BufferedWriter bw = new BufferedWriter(
                new FileWriter(mergedChunkOfFile))) {

            for (File mergingFile : mergingChunks) {
                brMap.put(new BufferedReader(new FileReader(mergingFile)),
                        mergingFile);
            }

            for (Iterator<Map.Entry<BufferedReader, File>> brIterator = brMap
                    .entrySet().iterator(); brIterator.hasNext();) {
                Map.Entry<BufferedReader, File> entry = brIterator.next();
                BufferedReader br = entry.getKey();
                String inputLine = br.readLine();
                if (inputLine == null) {
                    br.close();
                    File file = entry.getValue();
                    brIterator.remove();
                    file.delete();
                    continue;
                }
                priorityQueue.offer(
                        new Tuple<String, BufferedReader>(inputLine, br));
            }

            while (!brMap.isEmpty()) {

                Tuple<String, BufferedReader> tuple = priorityQueue.poll();
                bw.write(tuple.getT1());
                bw.newLine();

                BufferedReader br = tuple.getT2();
                String inputLine = br.readLine();
                if (inputLine == null) {
                    br.close();
                    File file = brMap.get(br);
                    brMap.remove(br);
                    file.delete();
                    continue;
                }
                tuple.setT1(inputLine);
                priorityQueue.offer(tuple);

            }

        } finally {
            for (BufferedReader br : brMap.keySet()) {
                if (br != null) {
                    br.close();
                }
            }
        }
    }

    @Override
    public void run() {

        Thread thread = Thread.currentThread();
        synchronized (threadSet) {
            threadSet.add(thread);
        }

        Semaphore setOfChunksReadySemaphore = Utils.getSetofchunksreadysemaphore();
        
        int maxNumOfMergingChunks = Utils.getMaxNumOfMergingChunks();

        BlockingQueue<File> sortedChunksQueue = Utils.getSortedChunksQueue();

        List<File> mergingChunks = new ArrayList<>(maxNumOfMergingChunks);

        while (!Utils.allChunksSorted.get()
                || Utils.numberOfChunksForMerging.get() != 1) {

            mergingChunks.clear();

            File chunkOfFile = null;

            synchronized(this) {
                if (!Utils.allChunksSorted.get()
                        && (Utils.numberOfChunksForMerging.get()
                                - numberOfMergingChunks
                                        .get() >= maxNumOfMergingChunks)
                        || Utils.allChunksSorted.get()
                                && (Utils.numberOfChunksForMerging.get()
                                        - numberOfMergingChunks
                                                .get() > 1)) {
                    while (Utils.numberOfChunksForMerging.get()
                            - mergingChunks.size() > 0
                            && mergingChunks.size() < maxNumOfMergingChunks) {
                        try {
                            chunkOfFile = sortedChunksQueue.poll(120,
                                    TimeUnit.SECONDS);
                        } catch (InterruptedException e1) {
                        }
                        if (chunkOfFile != null) {
                            mergingChunks.add(chunkOfFile);
                            numberOfMergingChunks.getAndIncrement();
                        } else if (Utils.allChunksSorted.get())
                            break;
                    }
                }
            }

            if (mergingChunks.size() > 1) {
                File mergedChunkOfFile;
                if (Utils.isVerbose())
                    System.out.println("mergesort: " + new Date()
                            + " : Merger begin to merge: " + mergingChunks);
                try {
                    mergedChunkOfFile = File.createTempFile("mrgsrt" + "_m_"
                            + mergeNumber.getAndIncrement() + "_", null,
                            Utils.getTmpDirFile());
                    merge(mergingChunks, mergedChunkOfFile);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                sortedChunksQueue.offer(mergedChunkOfFile);
                Utils.numberOfChunksForMerging
                        .getAndAdd(1 - mergingChunks.size());
                numberOfMergingChunks.getAndAdd(-mergingChunks.size());

                Utils.updateSetOfChunksSemaphore();

                if (Utils.isVerbose())
                    System.out.println("mergesort: " + new Date() + " : "
                            + Thread.currentThread()
                            + " : Merger merged chunks: " + mergingChunks
                            + " in " + mergedChunkOfFile);

            } else if (mergingChunks.size() == 1) {
                sortedChunksQueue.offer(mergingChunks.get(0));
                numberOfMergingChunks.getAndDecrement();
                
                Utils.updateSetOfChunksSemaphore();

                if (Utils.isVerbose())
                    System.out.println("mergesort: " + new Date()
                            + " : Merger return chunk: " + mergingChunks.get(0)
                            + " in mergingChunks");
            } else {
                try {
                    setOfChunksReadySemaphore.tryAcquire(120, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                }
            }
        }

        if ((Utils.allChunksSorted.get()
                && Utils.numberOfChunksForMerging.get() == 1)
                && Utils.allChunksMerged.compareAndSet(false, true)) {
            allChanksMergedSemaphore.release();
            if (Utils.isVerbose())
                System.out.println("mergesort: " + new Date()
                        + " : Merger merge all chunks: ");
        }

        synchronized (threadSet) {
            threadSet.remove(threadSet);
        }

    }
}
