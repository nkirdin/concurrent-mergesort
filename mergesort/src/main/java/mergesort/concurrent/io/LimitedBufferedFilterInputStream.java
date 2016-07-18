package mergesort.concurrent.io;

import java.io.FileInputStream;
import java.io.FilterInputStream;
import java.io.IOException;

/**
 * @author Nikolay Kirdin 2016-07-15
 * @version 0.2.1
 */
public class LimitedBufferedFilterInputStream extends FilterInputStream {

    public static final int BUFFER_SIZE_DEFAULT = 64 * 1024;

    private static final int MAX_STRING_ARRAY_LENGTH = 4 * 1024 * 1024;

    private static final int BYTE_ARRAY_EXT_LENGTH = 80;

    private int bufferSize;

    private byte[] buff;

    private long effectiveStreamPosition = 0;

    private int currentBuffIndex;

    private int lastIndexInBuff;

    private long endOfStreamPosition = Long.MAX_VALUE;

    public LimitedBufferedFilterInputStream(FileInputStream in) {
        this(in, BUFFER_SIZE_DEFAULT);
    }

    public LimitedBufferedFilterInputStream(FileInputStream in,
            int bufferSize) {
        super(in);
        this.bufferSize = bufferSize;
        buff = new byte[bufferSize];
        currentBuffIndex = bufferSize;
        lastIndexInBuff = currentBuffIndex;
    }

    @Override
    public int available() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void mark(int readlimit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public synchronized int read() throws IOException {
        if (effectiveStreamPosition == endOfStreamPosition) {
            currentBuffIndex = buff.length;
            lastIndexInBuff = 0;
            return -1;
        }

        if (currentBuffIndex == lastIndexInBuff) {
            int readBytes = super.read(buff, 0, buff.length);
            if (readBytes == -1) {
                currentBuffIndex = buff.length;
                lastIndexInBuff = 0;
                return -1;
            }
            currentBuffIndex = 0;
            lastIndexInBuff = readBytes;

        }
        effectiveStreamPosition++;
        return buff[currentBuffIndex++];
    }

    @Override
    public synchronized int read(byte[] byteArray) throws IOException {
        return super.read(byteArray);
    }

    @Override
    public synchronized int read(byte[] byteArray, int off, int len)
            throws IOException {
        if (effectiveStreamPosition == endOfStreamPosition) {
            currentBuffIndex = buff.length;
            lastIndexInBuff = 0;
            return -1;
        }

        if (effectiveStreamPosition != ((FileInputStream) in).getChannel()
                .position()) {
            ((FileInputStream) in).getChannel()
                    .position(effectiveStreamPosition);
            currentBuffIndex = bufferSize;
            lastIndexInBuff = currentBuffIndex;
        }

        int proposedLength = byteArray.length;
        if (effectiveStreamPosition + byteArray.length >= endOfStreamPosition) {
            proposedLength = (int) (endOfStreamPosition
                    - effectiveStreamPosition);
        }

        int readBytes = super.read(byteArray, 0, proposedLength);
        effectiveStreamPosition = ((FileInputStream) in).getChannel()
                .position();
        return readBytes;
    }

    @Override
    public synchronized void close() throws IOException {
        super.close();
        in = null;
    }

    @Override
    public synchronized void reset() throws IOException {
        setPosition(0L);
        resetBuffer(0L);
    }

    @Override
    public synchronized long skip(long n) throws IOException {
        if (n < 0)
            throw new IllegalArgumentException(
                    "Skip argument shuld be greater than 0");
        setPosition(effectiveStreamPosition + n);
        resetBuffer(getPosition());
        return getPosition();
    }

    public synchronized void setEndOfStreamPosition(long endOfStreamPosition) {
        if (in == null)
            throw new IllegalStateException("Stream is closed");
        if (endOfStreamPosition < 0)
            throw new IllegalArgumentException(
                    "EndOfStreamPosition shuld be greater than 0");
        this.endOfStreamPosition = endOfStreamPosition;
    }

    public synchronized long getEndOfStreamPsition() {
        if (in == null)
            throw new IllegalStateException("Stream is closed");
        return endOfStreamPosition;
    }

    public int getBufferSize() {
        if (in == null)
            throw new IllegalStateException("Stream is closed");
        return bufferSize;
    }

    public synchronized long getPosition() throws IOException {
        if (in == null)
            throw new IllegalStateException("Stream is closed");
        return ((FileInputStream) in).getChannel().position();
    }

    public synchronized void setPosition(long currentPosition)
            throws IOException {
        if (in == null)
            throw new IllegalStateException("Stream is closed");
        if (currentPosition < 0)
            throw new IllegalArgumentException(
                    "Position shuld be greater than 0");
        ((FileInputStream) in).getChannel().position(currentPosition);
        resetBuffer(currentPosition);
    }

    private synchronized void resetBuffer(long currentPosition) {
        currentBuffIndex = lastIndexInBuff;
        effectiveStreamPosition = currentPosition;
    }

    public synchronized long getEffectiveStreamPosition() {
        return effectiveStreamPosition;
    }

    /**
     * 
     * @return String
     * @throws IOException
     * @throws StringExceedMaxmumLengthException
     *             if read String is greater than MAX_STRING_ARRAY_LENGTH
     */
    public synchronized String readLine() throws IOException {
        byte[] byteArray = new byte[512];
        int index = 0;

        int readByte;
        while ((readByte = read()) != -1) {
            if (readByte == '\n') {
                break;
            }

            if (index == byteArray.length) {

                if (index + BYTE_ARRAY_EXT_LENGTH > MAX_STRING_ARRAY_LENGTH) {
                    throw new StringExceedMaxmumLengthException(
                            "String exceed: " + MAX_STRING_ARRAY_LENGTH
                                    + " byte");
                } else {
                    byteArray = extendByteArray(byteArray,
                            BYTE_ARRAY_EXT_LENGTH);
                }
            }

            byteArray[index++] = (byte) readByte;

        }
        return readByte != -1 ? new String(byteArray, 0, index)
                : index != 0 ? new String(byteArray, 0, index) : null;
    }

    /**
     * Extend byte Array
     * 
     * @param byteArray
     *            - extending array
     * @param extLength
     *            - additional length
     * @return new byte array with new length. New array is a copy of source
     *         array.
     */
    private byte[] extendByteArray(byte[] byteArray, int extLength) {
        byte[] newArray = new byte[byteArray.length + extLength];
        System.arraycopy(byteArray, 0, newArray, 0, extLength);
        return newArray;
    }
}
