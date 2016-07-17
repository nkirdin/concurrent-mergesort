package mergesort.concurrent;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Date;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * Модуль реализует одну из разновидностей сортировки слиянием (merge sort). При
 * выполнении сортировки файла он делится на несколько файлов заданного размера,
 * после этого эти файлы целиком загружаются в оперативную память и сортируются
 * там. Если используется несколько сортировщиков, то соответственно несколько
 * файлов будет загружаться в ОЗУ и сортироваться там одновременно. Поэтому
 * необходимо задавать разумные значения, как по размеру сортируемой части
 * файла, так и по количеству одновременно работающих сортировщиков, чтобы было
 * достаточно оперативной памяти для выполнения программы. После сортировки в
 * оперативной памяти отсортированный участок файла выгружается на диск. Если
 * таких отсортированных участков больше одного то они будут объединены т.н.
 * мержером. Работа программы завершается после того как сортируемый файл будет
 * полностью разделен на части, эти части будут отсортированный и объединены.
 * 
 * Программа разрабатывалась и тестировалась на строках в кодировке UTF-8,
 * которые завершаются символом '\n' (в соответствии с Unix-конвенцией).
 * 
 * 
 * Параметры выполнения:
 * 
 * java -jar mergesort.jar [-V] [-c <number_of_concurrently_merged_chunks> ]
 * [-h] -i <input file> [-m <RAM_per_one_sorter_thread_MBytes>] -o
 * <output_file> [-p <number_of_splitter_threads>] [-r
 * <number_of_merger_threads>] [-t <directory for temporary files>] [-v] [-x
 * <maximum_number_of_concurrently_working_threads>]
 * 
 * Параметры командной строки: -V - вывод дополнительная информации о работе
 * программы; -c - максимальное количество одновременно объединяемых файлов
 * одним мержером (20); -h - вывод краткой справки и информации об основных
 * рабочих параметрах; -i - исходный файл; -m - примерный объем доступной
 * оперативной памяти в мегабайтах на один сортировщик (50); -o -
 * отсортированный файл; -p - максимальное число одновременно работающих
 * сплиттеров (5); -r - максимальное количество одновременно работающих мержеров
 * (1); -t - директорий для размещения временных файлов (желательно с большими
 * IO/s), по умолчанию используется директорий получаемый из системной проперти
 * "java.io.tmpdir". Во время работы в этом директории размещаюся файлы с вида
 * `mrgsrt_s_<number>_<suffix>` на этапе разделения и упорядочения частей
 * исходного файла и `mrgsrt_m_<number>_ <suffix>` на этапах слияния, где `
 * <number>` - это порядковый номер операции, `<suffix>` - это
 * системногенерируемый суффикс для временных файлов; -v - вывод версии
 * программы; -x - максимальное количество одновременно исполняемых тредов (5).
 *
 * В скобках указаны значения по умолчанию. Ключи должны отделяться друг от
 * друга и от параметров пробелами.
 * 
 * @author Nikolay Kirdin 2016-07-17
 * @version 0.3
 */

public class MergeSort {

    public static void main(String[] args) {

        long startTime = System.currentTimeMillis();

        int maxNumOfMergingChunks = Utils.getMaxNumOfMergingChunks();
        String maxNumOfMergingChunksString = Integer
                .toString(maxNumOfMergingChunks);

        /*
         * RAM memory (MBytes) per one sorter thread by default
         */
        long ramValue = Utils.getMaxChunkFileLength();
        String ramValueString = Long.toString(ramValue / 1024 / 1024);

        /*
         * Number of concurrent readers of source file
         */
        int maxSplitterThreads = Utils.getMaxSplitterThreads();
        String maxSplitterThreadsString = Integer.toString(maxSplitterThreads);

        /*
         * Number of concurrent mergers
         */
        int maxMergerThreads = Utils.getMaxMergerThreads();
        String maxMergerThreadsString = Integer.toString(maxMergerThreads);

        /*
         * Maximum number of concurrently working threads
         */
        int maxNumberOfConcurrentThreads = Utils
                .getMaxNumberOfConcurrentThreads();
        String maxNumberOfConcurrentThreadsString = Integer
                .toString(maxNumberOfConcurrentThreads);
        /*
         * Available memory
         */
        long maxMemory = Runtime.getRuntime().totalMemory();

        String sourceString = "";
        File sourceFile = null;

        String output = "";

        String tmpDirString = "";
        File tmpDirFile = null;

        int k = 0;
        int resultOfCommadLineParsing = 0;

        for (; k < args.length;) {

            switch (args[k++]) {
            case "-V":
                Utils.setVerbose(true);
                break;
            case "-c": // c; d; v
                try {
                    maxNumOfMergingChunksString = args[k++];
                    maxNumOfMergingChunks = Integer
                            .parseInt(maxNumOfMergingChunksString);
                } catch (NumberFormatException nfe) {
                    resultOfCommadLineParsing |= 0x10;
                }
                break;
            case "-h":
                System.out.println(
                        "java -jar mergesort.jar  [-V] [-h] -i <input file> [-m <RAM_per_one_sorter_thread_MBytes>] -o <output_file> [-p <number_of_splitter_threads>] [-r <number_of_merger_threads>]  [-s <number_of_sorter_threads>] [-t <directory for temporary files>] [-v] [-x <maximum_number_of_concurrently_working_threads>]");
                break;
            case "-i": // c; d; v
                sourceString = args[k++];
                sourceFile = new File(sourceString);
                if (!sourceFile.exists()) {
                    resultOfCommadLineParsing |= 0x20;
                }
                break;
            case "-m":
                try {
                    ramValueString = args[k++];
                    ramValue = Integer.parseInt(ramValueString) * 1024 * 1024;
                } catch (NumberFormatException nfe) {
                    resultOfCommadLineParsing |= 0x40;
                }
                break;
            case "-o":
                output = args[k++];
                break;
            case "-p":
                try {
                    maxSplitterThreadsString = args[k++];
                    maxSplitterThreads = Integer
                            .parseInt(maxSplitterThreadsString);
                } catch (NumberFormatException nfe) {
                    resultOfCommadLineParsing |= 0x100;
                }
                break;
            case "-r":
                try {
                    maxMergerThreadsString = args[k++];
                    maxMergerThreads = Integer.parseInt(maxMergerThreadsString);
                } catch (NumberFormatException nfe) {
                    resultOfCommadLineParsing |= 0x200;
                }
                break;
            case "-t":
                tmpDirString = args[k++];
                tmpDirFile = new File(tmpDirString);
                if (!tmpDirFile.exists()) {
                    resultOfCommadLineParsing |= 0x800;
                }
                Utils.setTmpDirFile(tmpDirFile);
                break;
            case "-v":
                System.out.println("mergesort version: " + Utils.VERSION);
                break;
            // maxNumberOfConcurrentThreadsString
            case "-x":
                try {
                    maxNumberOfConcurrentThreadsString = args[k++];
                    maxNumberOfConcurrentThreads = Integer
                            .parseInt(maxNumberOfConcurrentThreadsString);
                } catch (NumberFormatException nfe) {
                    resultOfCommadLineParsing |= 0x1000;
                }
                break;

            default:
                System.out.println("ERROR: Unknown key: " + args[k - 1]);
                resultOfCommadLineParsing |= 0x800;
            }
        }

        /***************************** Diagnostics ***************************/
        if (maxNumOfMergingChunks < 2
                || ((resultOfCommadLineParsing & 0x10) != 0)) {
            System.out.println(
                    "ERROR: Illegal format for <number_of_concurrently_merged_chunks>. Should be positive integer greater than 1 : "
                            + maxNumOfMergingChunksString);
            resultOfCommadLineParsing |= 0x10;
        }

        if (sourceString.isEmpty() || (resultOfCommadLineParsing & 0x20) != 0) {
            System.out.println("ERROR: Wrong source file: " + sourceString);
            resultOfCommadLineParsing |= 0x20;
        }

        if (output.isEmpty()) {
            System.out.println("ERROR: Wrong output file name: " + output);
            resultOfCommadLineParsing |= 0x80;
        }

        if (maxSplitterThreads < 1
                || ((resultOfCommadLineParsing & 0x100) != 0)) {
            System.out.println(
                    "ERROR: Illegal format for <number_of_splitter_threads>. Should be positive integer: "
                            + maxSplitterThreadsString);
            resultOfCommadLineParsing |= 0x100;
        }

        if (maxMergerThreads < 1
                || ((resultOfCommadLineParsing & 0x200) != 0)) {
            System.out.println(
                    "ERROR: Illegal format for <number_of_merger_threads>. Should be positive integer: "
                            + maxMergerThreadsString);
            resultOfCommadLineParsing |= 0x200;
        }

        if ((resultOfCommadLineParsing & 0x800) != 0) {
            System.out.println(
                    "ERROR: Directory for temporary files doesn't exist: "
                            + tmpDirString);
        }

        if ((ramValue * maxSplitterThreads < 1)
                || (ramValue * maxSplitterThreads > maxMemory)
                || ((resultOfCommadLineParsing & 0x40) != 0)) {
            System.out.println(
                    "ERROR: Illegal format for <RAM_per_one_sorter_thread_MBytes>. Should be positive integer: "
                            + ramValueString);
            resultOfCommadLineParsing |= 0x40;
        }

        if (maxNumberOfConcurrentThreads < 1
                || ((resultOfCommadLineParsing & 0x1000) != 0)) {
            System.out.println(
                    "ERROR: Illegal format for <maximum_number_of_concurrently_working_threads>. Should be positive integer: "
                            + maxNumberOfConcurrentThreadsString);
            resultOfCommadLineParsing |= 0x1000;
        }

        /***************************** Diagnostics ***************************/

        /***************************** Verbose *******************************/

        if (Utils.isVerbose()) {
            System.out.println("Number of concurrent merging chunks: "
                    + maxNumOfMergingChunksString
                    + ((resultOfCommadLineParsing & 0x10) == 0 ? " correct"
                            : " incorrect"));
            System.out.println("Source file: " + sourceString
                    + ((resultOfCommadLineParsing & 0x20) == 0 ? " correct"
                            : " incorrect"));
            System.out.println("Memory per sorter(MB): " + ramValueString
                    + ((resultOfCommadLineParsing & 0x40) == 0 ? " correct"
                            : " incorrect"));
            System.out.println("Output file: " + output
                    + ((resultOfCommadLineParsing & 0x80) == 0 ? " correct"
                            : " incorrect"));
            System.out.println(
                    "Number of concurrent splitters of the input file: "
                            + maxSplitterThreadsString
                            + ((resultOfCommadLineParsing & 0x100) == 0
                                    ? " correct" : " incorrect"));
            System.out.println("Number of concurrent mergers of chunks: "
                    + maxMergerThreadsString
                    + ((resultOfCommadLineParsing & 0x200) == 0 ? " correct"
                            : " incorrect"));
            System.out.println("Directory for temporary files: " + tmpDirString
                    + ((resultOfCommadLineParsing & 0x800) == 0 ? " correct"
                            : " incorrect"));
            System.out.println("Number of concurrent threads: "
                    + maxNumberOfConcurrentThreadsString
                    + ((resultOfCommadLineParsing & 0x1000) == 0 ? " correct"
                            : " incorrect"));

            System.out.println("Requested memory (MB): "
                    + maxSplitterThreads * ramValue / 1024 / 1024);
            System.out
                    .println("Availabel RAM (MB): " + maxMemory / 1024 / 1024);
            if (resultOfCommadLineParsing != 0) {
                System.out.println("Exit by wrong command line parameters.");
            }
        }

        /***************************** Verbose *******************************/

        if (resultOfCommadLineParsing != 0) {
            System.exit(resultOfCommadLineParsing);
        }

        Utils.setMaxNumOfMergingChunks(maxNumOfMergingChunks); // -c

        Utils.setSourceFile(sourceFile); // -i
        Utils.setChunkFileLength(ramValue); // -m
                                            // -o
        Utils.setMaxSplitterThreads(maxSplitterThreads); // -p
        Utils.setMaxMergerThreads(maxMergerThreads); // -r

        Utils.setTmpDirFile(tmpDirFile); // -t
        Utils.setMaxNumberOfConcurrentThreads(maxNumberOfConcurrentThreads); // -x

        if (Utils.isVerbose())
            System.out.println(
                    "mergesort: " + new Date() + " : Start splitting points");

        SplitterSorter splitterSorter = new SplitterSorter();

        int numberOfSplittingIntervals = SplitterSorter.makePointsForSplitting(
                sourceFile, Utils.getMaxChunkFileLength(),
                Utils.getPointsForSplittingQueue());
        Utils.numberOfSplittingIntervals.set(numberOfSplittingIntervals);

        Utils.setSortedChunksQueue(new PriorityBlockingQueue<File>(
                numberOfSplittingIntervals, new FileLengthComparator()));

        if (Utils.isVerbose())
            System.out.println(
                    "mergesort: " + new Date() + " : Splitting points: "
                            + Utils.getPointsForSplittingQueue());

        MergerSortThreadFactory mergerSortThreadFactory = new MergerSortThreadFactory();
        ExecutorService executorService = Executors.newFixedThreadPool(
                maxNumberOfConcurrentThreads, mergerSortThreadFactory);

        if (Utils.isVerbose())
            System.out.println("mergesort: " + new Date()
                    + " : Start splitting and sorting file");
        for (int i = 0; i < maxSplitterThreads; i++) {
            executorService.execute(splitterSorter);
        }

        if (Utils.isVerbose())
            System.out.println(
                    "mergesort: " + new Date() + " : Start merging file");
        Merger merger = new Merger();
        for (int i = 0; i < maxMergerThreads; i++) {
            executorService.execute(merger);
        }

        try {
            Utils.checkSemaphoreAndHelth(
                    splitterSorter.getAllChanksSortedSemaphore(),
                    splitterSorter.getThreadSet(), "Sorting");

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
            System.out.println("mergesort: " + new Date() + " : ERROR : Inconsistency error.");
            System.exit(254);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("mergesort: " + new Date() + " : ERROR : Internal error.");
            System.exit(254);
        }

        int intervals = numberOfSplittingIntervals;
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

        Queue<File> sortedChunksQueue = Utils.getSortedChunksQueue();

        File mergedFile = sortedChunksQueue.poll();
        if (Utils.isVerbose())
            System.out.println("mergesort: " + new Date() + " : result file: "
                    + mergedFile);
        if (numberOfSplittingIntervals != splitterSorter
                .getNumberOfSortedChunks()
                || Utils.numberOfChunksForMerging.get() != 1
                || sortedChunksQueue.size() != 0 || mergedFile == null
                || numberOfMerges != Merger.getMergeNumber()) {
            System.out.println("mergesort: " + new Date() + " : ERROR : Severe internal error.");
            System.exit(255);
        }

        File outputFile = new File(output);

        if (Utils.isVerbose())
            System.out.println(
                    "mergesort: " + new Date() + " : Start moving file");

        try {
            Files.move(mergedFile.toPath(), outputFile.toPath(),
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (Utils.isVerbose())
            System.out
                    .println("mergesort: " + new Date() + " : End moving file");
        if (Utils.isVerbose())
            System.out.println("Sorting duration (s) : "
                    + (((double) System.currentTimeMillis()) - startTime)
                            / 1000);
        System.exit(0);
    }

}
