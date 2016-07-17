package mergesort.concurrent;

import java.util.concurrent.ThreadFactory;
/**
 * @author Nikolay Kirdin 2016-07-15
 * @version 0.2.1
 */
public class MergerSortThreadFactory implements ThreadFactory {

    @Override
    public Thread newThread(Runnable r) {
        Thread thread = new Thread(r);
        thread.setDaemon(true);
        return thread;
    }

}
