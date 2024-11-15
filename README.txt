How to run the program:
hadoop jar mr.jar MRMiner MINSUPP CORR TRANS_PER_BLOCK PATH_TO_INPUT PATH_TO_FIRST_OUT PATH_TO_FINAL_OUT

Note: I do not hard code the total number of transactions, so please clean up the input directory to make sure that input directory contains only one file which is the input file (please also delete all hidden files). The PATH_TO_INPUT should be just the directory path. For example, if the input directory is wocoin, use wocoin instead of wocoin/input_file.txt in place of PATH_TO_INPUT as a command line argument though using wocoin/input_file.txt seems to be working the same.



