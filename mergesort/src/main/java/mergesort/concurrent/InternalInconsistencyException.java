package mergesort.concurrent;

/**
 * @author Nikolay Kirdin 2016-07-15
 * @version 0.2.1
 */
public class InternalInconsistencyException extends Exception {

    private static final long serialVersionUID = 1L;

    public InternalInconsistencyException(String message) {
        super(message);
    }

}
