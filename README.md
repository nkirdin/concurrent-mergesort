##Concurrent merge sort

v. 0.3.0  
2016-07-17  

Программа является примером использования нескольких потоков для сортировки больших файлов, содержащих текстовую информацию. Она выделяет строки и сортирует их в лексикографическом порядке. Строки представляют собой последовательность символов в кодировке UTF-8 и разделяются, как принято в среде Unix-подобных ОС, символами '\n' (0x0A). Программа разработана как подобие утилиты sort.  

Программа разработана среде Linux, для использования в среде Java SE 8 и собирается с помощью менеджера сборки maven 3.   

Программа  реализует одну из разновидностей сортировки слиянием (merge sort). При выполнении сортировки файла он делится на несколько частей, заданного размера, после этого эти части целиком загружаются в оперативную память и сортируются там. Если используется несколько сортировщиков, то соответственно несколько файлов будет загружаться в ОЗУ и сортироваться там одновременно. Поэтому необходимо задавать разумные значения, как по размеру сортируемой части файла, так и по количеству одновременно работающих сортировщиков, чтобы было достаточно оперативной памяти для выполнения программы. После сортировки в оперативной памяти отсортированный участок файла выгружается на диск. Если таких отсортированных участков больше одного, то они будут объединены. Работа программы завершается после того как сортируемый файл будет полностью разделен на части, эти части будут отсортированный и объединены.  
  
Параметры выполнения:  
  
 `java -jar mergesort.jar  [-V] [-c <number_of_concurrently_merged_chunks> ] [-h] -i <input file> [-m <RAM_per_one_sorter_thread_MBytes>] -o <output_file> [-p <number_of_splitter_threads>] [-r <number_of_merger_threads>]  [-t <directory_for_temporary_files>] [-v] [-x <maximum_number_of_concurrently_working_threads>]` 

* Параметры командной строки: 
* -V - вывод подробной дополнительная информации о работе программы;
* -c - максимальное количество одновременно объединяемых файлов одним мержером (20); 
* -h - вывод краткой справки и информации об основных рабочих параметрах; 
* -i - исходный файл; 
* -m - примерный объем доступной оперативной памяти в мегабайтах на один сортировщик (50);
* -o - отсортированный файл; 
* -p - максимальное число одновременно работающих сплиттеров (5); 
* -r - максимальное количество одновременно работающих мержеров (1); 
* -t - директорий для размещения временных файлов (желательно с большими IO/s), по умолчанию используется  директорий получаемый из системной проперти "java.io.tmpdir". Во время работы в этом директории размещаюся файлы с вида `mrgsrt_s_<number>_<suffix>` на этапе разделения и упорядочения частей исходного файла и `mrgsrt_m_<number>_ <suffix>` на этапах слияния, где `<number>` - это порядковый номер операции, `<suffix>` - это системногенерируемый суффикс для временных файлов; 
* -v - вывод версии программы;
* -x - максимальное количество одновременно исполняемых тредов (5).
 
В скобках указаны значения по умолчанию.  Ключи должны отделяться друг от друга и от параметров пробелами.
  
Среди тестов находится модуль GeneratorOfTestFile, который может быть использован для генерации тестовых файлов. В результате работы модуля создается файл с указывемым в командной строке именем, по умолчанию имя "TestFile.txt". Формат тестового файла следующий. В первых 12 позициях выводится выравненноя вправо десятичное число, при необходимости оно дополняется нолями слева. После числа выводится символ двоеточие ":" после него случайная последовательность десятичных чисел случайной длины. Максимальная длина этой последовательности определяется в командной строке, по умолчанию она равна 80 символам. С помощью параметров командной строки можно указать максимальную длину файла, по умолчанию она равна 64 Кбайт. Файл может быть создан немного длиннее чем заданная длина. С помощью командной строки можно изменить параметры внесения случайных изменений.
  
Файл создается с использованием кодировки UTF-8. Строки завершаются в UNIX-стиле символом '\n'.  
  
Параметры командной строки:  
`[-D] [-L] [-S] [-h] [-l <file_length_bytes>] [-o <file_name>] [-s <maximum_length_of_string_symbols>]`  
* -D - отключение перемешивания символов внутри строки;  
* -L - отключение генератора случайной длины строки;  
* -S - отключение генератора перемешивания строк;  
* -h - вывод краткой справки;  
* -l - примерная длина тествого файла в байтах. Получаемый файл может быть немного длиннее. По умолчанию длина равна 64 Кбайт;
* -o - имя генерируемого файла; 
* -s - максимальная длина строки. По умолчанию она равна 80 символам.
  
Приложение разработано в среде Linux и предназначено для работы в среде Java SE 8. Исходные тексты предоставляются в виде проекта для утилиты сборки "Apache maven v.3.0.5". Для сборки приложения необходимо использовать корневой pom-файл.

Отказ от гарантий  
Ограничение ответственности  
Разработчик: Николай Кирдин

email: nkirdin@gmail.com

©Nikolay Kirdin, 2016

