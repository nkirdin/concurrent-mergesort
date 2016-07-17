package mergesort.concurrent;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

/**
 * Модуль предназначен для подготовки тестовых файлов, которые будут
 * использоваться для тестирования программы сортировки. В результате работы
 * модуля создается файл с указывемым в командной строке именем, по умолчанию
 * имя "TestFile.txt". Формат тестового файла следующий. В первых 12 позициях
 * выводится выравненноя вправо десятичное число, при необходимости оно
 * дополняется нолями слева. После числы выводится символ двоеточие ":" и после
 * него случайная последовательность десятичных чисел случайной длины.
 * Максимальная длина этой последовательности определяется в командной строке,
 * по умолчанию она равна 80 символам. С помощью параметров командной строки
 * можно указать максимальную длину файла, по умолчанию она равна 64 Кб. Файл
 * может быть создан немного длиннее чем заданная длина. С помощью командной
 * строки можно изменить параметры внесения случайных изменений.
 * 
 * Файл создается с использованием кодировки UTF-8. Строки завершаются в
 * UNIX-стиле символом '\n'.
 * 
 * Параметры командной строки: [-D] [-L] [-S] [-h] [-l <file_length_bytes>] [-o
 * <file_name>] [-s <maximum_length_of_string_symbols>] -D - отключение
 * перемешивания символов внутри строки; -L - отключение генератора случайной
 * длины строки; -S - отключение генератора перемешивания строк; -h - вывод
 * краткой справки; -l - примерная длина тествого файла в байтах. Получаемый
 * файл может быть немного длиннее. По умолчанию длина равна 64 Кбайт; -o - имя
 * генерируемого файла; -s - максимальная длина строки. По умолчанию она равна
 * 80 символам.
 * 
 * @author Nikolay Kirdin 2016-07-16
 * @version 0.3
 */
public class GeneratorOfTestFile {

    public static void main(String[] args) {
        long maxFileLength = 64 * 1024;
        int maxStringLength = 80;
        String testFileName = "TestFile.txt";
        boolean shuffling = true;
        boolean lineLengthModifying = true;
        boolean lineSymbolsShuffling = true;

        int k = 0;

        for (; k < args.length;) {
            switch (args[k++]) {
            case "-l":
                try {
                    maxFileLength = Long.parseLong(args[k++]);
                } catch (NumberFormatException nfe) {
                    System.out.println("ERROR: Illegal Max file lenth");
                    System.out.println(
                            "Usage: GenTestFile [-D] [-L] [-S] [-h] [-l <file_length_bytes>] [-o <file_name>] [-s <maximum_length_of_string_symbols>]");
                    System.exit(1);
                }
                break;
            case "-s":
                try {
                    maxStringLength = Integer.parseInt(args[k++]);
                } catch (NumberFormatException nfe) {
                    System.out.println("ERROR: Illegal Max line lenth");
                    System.exit(2);
                }
                break;
            case "-o":
                testFileName = args[k++];
                break;
            case "-h":
                System.out.println(
                        "Usage: GenTestFile [-l <File length>] [-x <Max line length>] [-o <Output file name>]");
                System.exit(0);
                break;
            case "-S":
                shuffling = false;
                break;
            case "-L":
                lineLengthModifying = false;
                break;
            case "-D":
                lineSymbolsShuffling = false;
                break;
            default:
                System.out.println("ERROR: Unknown key");
            }

        }

        if ((maxFileLength < maxStringLength + 13) || maxFileLength <= 0
                || maxStringLength <= 0) {
            System.out.println(
                    "ERROR: Wrong maximum file length and/or maximum line length");
            System.exit(4);
        }

        File file = new File(testFileName);

        try (BufferedWriter bw = new BufferedWriter(new FileWriter(file))) {

            long fileLength = 0;
            Random randomLength = new Random();
            Random randomDigit = new Random();
            long stringNumber = 0;
            long prevPercent = 0;
            List<String> stringList = new LinkedList<>();

            while (fileLength < maxFileLength) {
                StringBuilder outputString = new StringBuilder();

                outputString.append(String.format("%012d:", stringNumber++));

                int stringLength = 13;
                int randomStringLength = lineLengthModifying
                        ? randomLength.nextInt(maxStringLength)
                        : maxStringLength;
                int digit = 3;
                while (stringLength < randomStringLength) {
                    stringLength++;
                    if (lineSymbolsShuffling)
                        digit = randomDigit.nextInt(10);
                    else {
                        digit++;
                        digit %= 10;
                    }
                    outputString.append(Integer.toString(digit));
                }
                fileLength += stringLength;
                stringList.add(outputString.toString());
                outputString.delete(0, outputString.length());

                if (stringNumber % 10000 == 0) {
                    long currentPercent = Math
                            .round((fileLength * 100.0) / maxFileLength);

                    if (currentPercent - prevPercent > 10) {
                        System.out.print("%");
                        prevPercent = currentPercent;
                    }
                    if (shuffling)
                        Collections.shuffle(stringList);
                    for (String string : stringList) {
                        bw.write(string.toString());
                        bw.newLine();
                    }
                    stringList.clear();
                }
            }

            if (!stringList.isEmpty()) {
                if (shuffling)
                    Collections.shuffle(stringList);
                for (String string : stringList) {
                    bw.write(string.toString());
                    bw.newLine();
                }
            }
            System.out.println("\nEnd of generation");

            bw.flush();
            bw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
