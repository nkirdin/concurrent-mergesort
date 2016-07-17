package mergesort.concurrent.io;

import static org.junit.Assert.assertEquals;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;

import org.junit.Test;

/**
 * @author Nikolay Kirdin 2016-07-15
 * @version 0.2.1
 */
public class LimitedBufferedFilterInputStreamTest {

    @Test
    public void readBufferTest() throws IOException {
        String testPath = ClassLoader.getSystemResource("SplitterTest_40K.txt")
                .getPath();
        File testFile = new File(testPath);
        try (FileInputStream inputStream = new FileInputStream(testFile);
                LimitedBufferedFilterInputStream lfis = new LimitedBufferedFilterInputStream(
                        inputStream)) {

            byte[] bytes = new byte[16];
            int readedBytes = lfis.read(bytes);
            assertEquals(16, readedBytes);
            assertEquals("000000000000:456", new String(bytes));

            lfis.reset();
            assertEquals(0L, lfis.getPosition());
            assertEquals(0L, lfis.getEffectiveStreamPosition());

            lfis.skip(0L);
            lfis.read(bytes);
            assertEquals("000000000000:456", new String(bytes));

            lfis.reset();
            assertEquals(0L, lfis.getPosition());
            assertEquals(0L, lfis.getEffectiveStreamPosition());
            lfis.skip(65L);
            assertEquals(65L, lfis.getPosition());
            assertEquals(65L, lfis.getEffectiveStreamPosition());
            lfis.read(bytes);
            assertEquals("000000000001:456", new String(bytes));

            lfis.skip(49L);
            assertEquals(130L, lfis.getPosition());
            assertEquals(130L, lfis.getEffectiveStreamPosition());
            lfis.read(bytes);
            assertEquals("000000000002:456", new String(bytes));

            lfis.setPosition(100L);
            assertEquals(100L, inputStream.getChannel().position());

            lfis.skip(70L);
            assertEquals(170L, inputStream.getChannel().position());
        }
    }

    @Test
    public void readFileTest() throws IOException {
        String testPath = ClassLoader.getSystemResource("SplitterTest_40K.txt")
                .getPath();
        File testFile = new File(testPath);
        try (FileInputStream inputStream = new FileInputStream(testFile);
                LimitedBufferedFilterInputStream lfis = new LimitedBufferedFilterInputStream(
                        inputStream)) {

            try (BufferedInputStream bis = new BufferedInputStream(
                    new FileInputStream(testFile))) {
                int readBis;
                while ((readBis = bis.read()) != -1) {
                    assertEquals(readBis, lfis.read());
                }
            }
        }
    }

    @Test
    public void readLineTest() throws IOException {
        String testPath = ClassLoader.getSystemResource("SplitterTest_40K.txt")
                .getPath();
        File testFile = new File(testPath);
        try (FileInputStream inputStream = new FileInputStream(testFile);
                LimitedBufferedFilterInputStream lfis = new LimitedBufferedFilterInputStream(
                        inputStream)) {
            lfis.skip(testFile.length());
            assertEquals(null, lfis.readLine());
            lfis.reset();
            assertEquals(
                    "000000000000:456789012345678901234567890123456789012345678901234",
                    lfis.readLine());
            assertEquals(
                    "000000000001:456789012345678901234567890123456789012345678901234",
                    lfis.readLine());
            lfis.skip(130L);
            assertEquals(
                    "000000000004:456789012345678901234567890123456789012345678901234",
                    lfis.readLine());
        }
    }

    @Test
    public void readLineFileTest() throws IOException {
        String testPath = ClassLoader.getSystemResource("SplitterTest_40K.txt")
                .getPath();
        File testFile = new File(testPath);
        try (FileInputStream inputStream = new FileInputStream(testFile);
                LimitedBufferedFilterInputStream lfis = new LimitedBufferedFilterInputStream(
                        inputStream);
                BufferedReader br = new BufferedReader(
                        new FileReader(testFile))) {
            String s;
            while ((s = br.readLine()) != null) {
                assertEquals(s, lfis.readLine());
            }
        }
    }
}
