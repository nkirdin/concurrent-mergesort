package mergesort.concurrent;

import java.io.File;
import java.util.Comparator;

/**
 * @author Nikolay Kirdin 2016-07-15
 * @version 0.2.1
 */
public class FileLengthComparator implements Comparator<File> {

    @Override
    public int compare(File o1, File o2) {
        return Long.compare(o1.length(), o2.length());
    }

}
