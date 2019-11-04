MySQL Import tool for rezoJDM. 

The program will fetch the last avalaible dump (in zip format, 1+GB) from jeuxdemots (http://www.jeuxdemots.org/JDM-LEXICALNET-FR/) and import it into a new MySQL database.

**MySQL must be installed and added to the system PATH variable.**

You might want to modify your configuration file ("my.ini", probably in C:\ProgramData\MySQL\MySQL Server [VERSION]\my.ini") and increase both "innodb_buffer_pool_size" and "innodb_log_file_size".


There is no mandatory argument but you might need (or just want) to set some of them.

**MySQL related parameters:** 
*        -d/--database [DATABASE_NAME]: Database name (DEFAULT="rezoJDM)"
*        -u/--username [USERNAME]: MySQL username (DEFAULT="root")
*       -p/--password [PASSWORD]: MySQL password, leave empty if there is no password (DEFAULT="")
*        -u/--username [USERNAME]: MySQL username (DEFAULT="root")
*       --drop: Drop previous database with the same name (DEFAULT="false")

**Other parameters:**
 *       -i/--init [INIT_FILEPATH]: Filepath of the sql init file (DEFAULT="init.sql")
 *       -t/--temp [TEMPORARY_DOWNLOAD_DIRPATH]: Filepath of the temporary directory storing the dump and the csv files (DEFAULT="__tmpRezoJDMCSV")
 *       --keep: Do not delete the temporary folder and all its content before exiting (DEFAULT="false")
 *       --no-download: Do not attempt to download the lastest dump and instead try to read existing file from the temporary folder (DEFAULT="false")