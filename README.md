MySQL Import tool for rezoJDM. Works for both UNIX and Windows system. On UNIX you may need to use root. You will need root privileges on your MySQL user.

The program will fetch the last avalaible dump (in zip format, 1+GB) from jeuxdemots (http://www.jeuxdemots.org/JDM-LEXICALNET-FR/) and import it into a new MySQL database.

**Be careful, as of November 2019, the dump is a 13GB file and you will need around twice (thrice if you want to keep intermediate files) as much for the program to function correctly.**


## Building
The importer is bundled a maven project, that can generated an executable jar for convenience.
You need maven v3+ and java 1.8+. 

To build the jar: 
```$bash
mvn package
``` 

The jar is generated in `target/JDMImport.jar`, the main class declared in the manifest is `org.jeuxdemots.CreateAndLoad`. 


##Running

To run the importer you may use the executable jar 
```$bash
java -jar target/JDMImport-jar-with-dependencies.jar [ARGS]
```

**MySQL must be installed and added to the system PATH variable.**

You might want to modify your configuration file ("my.ini", probably in C:\ProgramData\MySQL\MySQL Server [VERSION]\my.ini") and increase both "innodb_buffer_pool_size" and "innodb_log_file_size".


There is no mandatory argument but you might need (or just want) to set some of them.

**MySQL related parameters:** 
 *        -d/--database DATABASE_NAME: Database name (DEFAULT="rezoJDM)"
 *        -u/--username USERNAME: MySQL username (DEFAULT="root")
 *       -p/--password PASSWORD: MySQL password, leave empty if there is no password (DEFAULT="")
 *       -H/--host HOST: MySQL server hostname (DEFAULT="localhost")
 *       -P/--port PORT: MySQL server port (DEFAULT="3306")
 *       --drop: Drop previous database with the same name (DEFAULT="false")

**Other parameters:**
 *       --log: Create logs for ouputs and errors from MySQL queries (DEFAULT="false")
 *       -i/--init INIT_FILEPATH: Filepath of the sql init file (DEFAULT="init.sql")
 *       -u/--update UPDATE_FILEPATH: Filepath of the sql update file (DEFAULT="update.sql")
 *       -t/--temp TEMPORARY_DOWNLOAD_DIRPATH: Filepath of the temporary directory storing the dump and the csv files (DEFAULT="__tmpRezoJDMCSV")
 *       --keep: Do not delete the temporary folder and all its content before exiting (DEFAULT="false")
 *       --no-download: Do not attempt to download the lastest dump and instead try to read existing file from the temporary folder (DEFAULT="false")
 *       -s/--size PARTITION_SIZE: Number of elements in each subfiles use to import nodes and edges.
        A powerfull machine might not need to split the csv files but in most cases, the entire dump cannot be imported all at once.
        Be careful as using a value too low might create a lot of files.
        Use 0 to not split any files (DEFAULT=100000)