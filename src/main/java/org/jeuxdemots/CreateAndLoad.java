package org.jeuxdemots;

import java.io.*;
import java.lang.ProcessBuilder.Redirect;
import java.text.DecimalFormat;
import java.util.Date;

public class CreateAndLoad {

    private static String HOST = "localhost";
    private static String PORT = "3306";
    private static String DB = "rezoJDM"; //-d or --database
    private static String USERNAME = "root"; //-u or --username
    private static String PASSWORD = ""; //-p or --password


    private static boolean DOWNLOAD_LAST_DUMP = true; //--no-download (flag)
    private static boolean DROP_IF_EXIST = false; // --drop (flag)
    private static boolean CLEAN_AFTER = true; // --keep (flag)
    private static boolean LOG_MYSQL = false; // --log (flag)
    private static String INIT_FILEPATH = "init.sql"; //-i or --init
    private static String UPDATE_FILEPATH = "update.sql"; //-u or --update
    private static String TEMP_CSV_FOLDER = "__tmpRezoJDMCSV"; //-t or --temp
    private static int PARTITIONS_SIZE = 100_000; //-s or --size


    public static void main(String[] args) {
        argsProcess(args);
        boolean hasPassword = !PASSWORD.isEmpty();
        File mysqlErrorLog, mysqlOutputLog;
        String logMessage;
        mysqlErrorLog = new File("JDMImport_mysqlErrorLog.log");
        mysqlOutputLog = new File("JDMImport_mysqlOutputLog.log");
        if (LOG_MYSQL) {
            Date logDate = new Date();
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(mysqlErrorLog, true))) {
                writer.write("********** JDMImport: Errors from SQL queries **********");
                writer.newLine();
                writer.write("Session: " + logDate);
                writer.newLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(mysqlOutputLog, true))) {
                writer.write("********** JDMImport: Ouputs from SQL queries **********");
                writer.newLine();
                writer.write("Session: " + logDate);
                writer.newLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        File tempFolder;
        long timer, importTimer;
        DecimalFormat format = new DecimalFormat();
        ProcessBuilder processBuilder;
        String query, basepathCsvFile;
        int part, nodeParts, edgeParts;
        timer = System.currentTimeMillis();

        //SQL_MODE
        logMessage = "Get sql_mode value: ";
        System.out.print(logMessage);
        File sql_mode = new File("sql_mode");
        String sqlModes = "";
        query = "show variables where Variable_name='sql_mode';";
        if (hasPassword) {
            processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-p\"" + PASSWORD + "\"", "-h", HOST, "-P", PORT, "-e", query);
        } else {
            processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-h", HOST, "-P", PORT, "-e", query);
        }
        try {
            if (LOG_MYSQL) {
                append(logMessage + System.lineSeparator(), mysqlErrorLog);
                processBuilder.redirectError(Redirect.appendTo(mysqlErrorLog));
            }
            processBuilder.redirectOutput(sql_mode);
            processBuilder.start().waitFor();
            try (BufferedReader reader = new BufferedReader(new FileReader(sql_mode))) {
                String line2 = reader.readLine(); //header
                if (line2 != null) {
                    line2 = reader.readLine();
                    if (line2 != null && line2.startsWith("sql_mode\t")) {
                        sqlModes = line2.substring(9).trim();
                    }
                }
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(sqlModes);
        if (!sqlModes.contains("NO_AUTO_VALUE_ON_ZERO")) {
            logMessage = "Temporarily adding 'NO_AUTO_VALUE_ON_ZERO' in sql_mode";
            System.out.println(logMessage);
            query = "set global sql_mode='NO_AUTO_VALUE_ON_ZERO";
            if (!sqlModes.isEmpty()) {
                query += "," + sqlModes;
            }
            query += "';";
            if (hasPassword) {
                processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-p\"" + PASSWORD + "\"", "-h", HOST, "-P", PORT, "-e", query);
            } else {
                processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-h", HOST, "-P", PORT, "-e", query);
            }
            try {
                if (LOG_MYSQL) {
                    append(logMessage + System.lineSeparator(), mysqlErrorLog);
                    append(logMessage + System.lineSeparator(), mysqlOutputLog);
                    processBuilder.redirectError(Redirect.appendTo(mysqlErrorLog));
                    processBuilder.redirectOutput(Redirect.appendTo(mysqlOutputLog));
                }
                processBuilder.start().waitFor();
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }


        logMessage = "Get local_infile value: ";
        System.out.print(logMessage);
        File localInfile = new File("local_infile_value");
        boolean localInfileValue = false;
        String line;
        query = "show variables where Variable_name='local_infile';";
        if (hasPassword) {
            processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-p\"" + PASSWORD + "\"", "-h", HOST, "-P", PORT, "-e", query);
        } else {
            processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-h", HOST, "-P", PORT, "-e", query);
        }
        try {
            if (LOG_MYSQL) {
                append(logMessage + System.lineSeparator(), mysqlErrorLog);
                processBuilder.redirectError(Redirect.appendTo(mysqlErrorLog));
            }
            processBuilder.redirectOutput(localInfile);
            processBuilder.start().waitFor();
            try (BufferedReader reader = new BufferedReader(new FileReader(localInfile))) {
                line = reader.readLine(); //header
                if (line != null) {
                    line = reader.readLine();
                    if (line != null && line.startsWith("local_infile\t")) {
                        line = line.substring(13);
                        localInfileValue = line.toUpperCase().equals("ON");
                    }
                }
            }
            localInfile.delete();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(String.valueOf(localInfileValue));

        if (!localInfileValue) {
            logMessage = "Temporarily setting local_infile value as 'true'";
            System.out.println(logMessage);
            query = "set global local_infile=1;";
            if (hasPassword) {
                processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-p\"" + PASSWORD + "\"", "-h", HOST, "-P", PORT, "-e", query);
            } else {
                processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-h", HOST, "-P", PORT, "-e", query);
            }
            try {
                if (LOG_MYSQL) {
                    append(logMessage + System.lineSeparator(), mysqlErrorLog);
                    append(logMessage + System.lineSeparator(), mysqlOutputLog);
                    processBuilder.redirectError(Redirect.appendTo(mysqlErrorLog));
                    processBuilder.redirectOutput(Redirect.appendTo(mysqlOutputLog));
                }
                processBuilder.start().waitFor();
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }

        //SQL_MODE
        //		logMessage = "Get sql_mode value: ";
        //		System.out.print(logMessage);
        //		File sql_mode = new File("local_infile_value");
        //		String sqlModes = "";
        //		query = "show variables where Variable_name='sql_mode';";
        //		if(hasPassword) {
        //			processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-p\""+PASSWORD+"\"", "-e", query);
        //		}else {
        //			processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-e", query);
        //		}
        //		try {
        //			if(LOG_MYSQL) {
        //				append(logMessage + System.lineSeparator(), mysqlErrorLog);
        //				processBuilder.redirectError(Redirect.appendTo(mysqlErrorLog));
        //			}
        //			processBuilder.redirectOutput(sql_mode);
        //			processBuilder.start().waitFor();
        //			try(BufferedReader reader = new BufferedReader(new FileReader(sql_mode))){
        //				line = reader.readLine(); //header
        //				if(line != null) {
        //					line = reader.readLine();
        //					if(line != null && line.startsWith("sql_mode\t")) {
        //						sqlModes = line.substring(9);
        //					}
        //				}
        //			}
        //			localInfile.delete();
        //		} catch (IOException | InterruptedException e) {
        //			e.printStackTrace();
        //		}
        //		System.out.println(String.valueOf(localInfileValue));

        //		if(!localInfileValue) {
        //			logMessage = "Temporarily setting local_infile value as 'true'";
        //			System.out.println(logMessage);
        //			query = "set global local_infile=1;";
        //			if(hasPassword) {
        //				processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-p\""+PASSWORD+"\"", "-e", query);
        //			}else {
        //				processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-e", query);
        //			}
        //			try {
        //				if(LOG_MYSQL) {
        //					append(logMessage + System.lineSeparator(), mysqlErrorLog);
        //					append(logMessage + System.lineSeparator(), mysqlOutputLog);
        //					processBuilder.redirectError(Redirect.appendTo(mysqlErrorLog));
        //					processBuilder.redirectOutput(Redirect.appendTo(mysqlOutputLog));
        //				}
        //				processBuilder.start().waitFor();
        //			} catch (IOException | InterruptedException e) {
        //				e.printStackTrace();
        //			}
        //		}


        //DL
        if (DOWNLOAD_LAST_DUMP) {
            System.out.println("Downloading dump and converting it into CSV files (this may take a few minutes)... ");
            DownloadAndConvert.downloadAndCSVConvert(TEMP_CSV_FOLDER, CLEAN_AFTER, PARTITIONS_SIZE);
        } else {
            System.out.println("Skipping dump download...");
        }

        //DROP DB
        if (DROP_IF_EXIST) {
            logMessage = "Dropping previous database (if it exists)... ";
            System.out.println(logMessage);
            query = "DROP DATABASE IF EXISTS " + DB + ";";
            if (hasPassword) {
                processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-p\"" + PASSWORD + "\"", "-h", HOST, "-P", PORT, "-e", query);
            } else {
                processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-h", HOST, "-P", PORT, "-e", query);
            }
            try {
                if (LOG_MYSQL) {
                    append(logMessage + System.lineSeparator(), mysqlErrorLog);
                    append(logMessage + System.lineSeparator(), mysqlOutputLog);
                    processBuilder.redirectError(Redirect.appendTo(mysqlErrorLog));
                    processBuilder.redirectOutput(Redirect.appendTo(mysqlOutputLog));
                }
                processBuilder.start().waitFor();
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("done!");
        }


        //CREATE DB
        logMessage = "Creating if not exists db=\"" + DB + "\"... ";
        System.out.print(logMessage);
        query = "CREATE DATABASE IF NOT EXISTS " + DB + " CHARACTER SET='utf8' COLLATE='utf8_bin';";
        if (hasPassword) {
            processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-p\"" + PASSWORD + "\"", "-h", HOST, "-P", PORT, "-e", query);
        } else {
            processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-h", HOST, "-P", PORT, "-e", query);
        }
        try {
            if (LOG_MYSQL) {
                append(logMessage + System.lineSeparator(), mysqlErrorLog);
                append(logMessage + System.lineSeparator(), mysqlOutputLog);
                processBuilder.redirectError(Redirect.appendTo(mysqlErrorLog));
                processBuilder.redirectOutput(Redirect.appendTo(mysqlOutputLog));
            }
            processBuilder.start().waitFor();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.out.println("done!");

        //TABLES INITIALISATION
        File sqlFile = new File(INIT_FILEPATH);
        if (sqlFile.exists()) {
            logMessage = "Tables initialisation from file \"" + sqlFile.getName() + "\"... ";
            System.out.print(logMessage);
            query = "source " + sqlFile.getAbsolutePath();
            if (hasPassword) {
                processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-p\"" + PASSWORD + "\"", "-h", HOST, "-P", PORT, DB, "-e", query);
            } else {
                processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-h", HOST, "-P", PORT, DB, "-e", query);
            }
            try {
                if (LOG_MYSQL) {
                    append(logMessage + System.lineSeparator(), mysqlErrorLog);
                    append(logMessage + System.lineSeparator(), mysqlOutputLog);
                    processBuilder.redirectError(Redirect.appendTo(mysqlErrorLog));
                    processBuilder.redirectOutput(Redirect.appendTo(mysqlOutputLog));
                }
                processBuilder.start().waitFor();
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
                System.exit(1);
            }
            System.out.println("done!");
        } else {
            System.out.println("Skipped Table initialisation because \"" + sqlFile.getAbsolutePath() + "\" is missing...");
        }


        System.out.println("Importing data: ");

        //Remove foreign key checks
        logMessage = "\tRemoving foreign key checks to speed up the process... ";
        System.out.print(logMessage);
        query = "SET GLOBAL foreign_key_checks = 0;";
        if (hasPassword) {
            processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-p\"" + PASSWORD + "\"", "-h", HOST, "-P", PORT, DB, "-e", query);
        } else {
            processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-h", HOST, "-P", PORT, DB, "-e", query);
        }
        try {
            if (LOG_MYSQL) {
                append(logMessage + System.lineSeparator(), mysqlErrorLog);
                append(logMessage + System.lineSeparator(), mysqlOutputLog);
                processBuilder.redirectError(Redirect.appendTo(mysqlErrorLog));
                processBuilder.redirectOutput(Redirect.appendTo(mysqlOutputLog));
            }
            processBuilder.start().waitFor();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.out.println("done!");

        //Remove unique checks
        logMessage = "\tRemoving unique checks to speed up the process... ";
        System.out.print(logMessage);
        query = "SET GLOBAL unique_checks = 0;";
        if (hasPassword) {
            processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-p\"" + PASSWORD + "\"", "-h", HOST, "-P", PORT, DB, "-e", query);
        } else {
            processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-h", HOST, "-P", PORT, DB, "-e", query);
        }
        try {
            if (LOG_MYSQL) {
                append(logMessage + System.lineSeparator(), mysqlErrorLog);
                append(logMessage + System.lineSeparator(), mysqlOutputLog);
                processBuilder.redirectError(Redirect.appendTo(mysqlErrorLog));
                processBuilder.redirectOutput(Redirect.appendTo(mysqlOutputLog));
            }
            processBuilder.start().waitFor();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.out.println("done!");

        logMessage = "\tnode_types... ";
        System.out.print(logMessage);
        sqlFile = new File(TEMP_CSV_FOLDER + File.separator + "nodeTypes.csv");
        if (sqlFile.exists()) {
            query = "load data local infile '" + TEMP_CSV_FOLDER + "/nodeTypes.csv" + "' " +
                    "into table node_types " +
                    "fields " +
                    "terminated by '|' " +
                    "IGNORE 1 LINES " +
                    "(id,name,info)";
            if (hasPassword) {
                processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-p\"" + PASSWORD + "\"", "-h", HOST, "-P", PORT, "--local-infile", DB, "-e", query);
            } else {
                processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-h", HOST, "-P", PORT, "--local-infile", DB, "-e", query);
            }
            try {
                if (LOG_MYSQL) {
                    append(logMessage + System.lineSeparator(), mysqlErrorLog);
                    append(logMessage + System.lineSeparator(), mysqlOutputLog);
                    processBuilder.redirectError(Redirect.appendTo(mysqlErrorLog));
                    processBuilder.redirectOutput(Redirect.appendTo(mysqlOutputLog));
                }
                processBuilder.start().waitFor();
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("done!");
        } else {
            System.out.println("Skipped node_types import because \"" + sqlFile.getAbsolutePath() + "\" is missing... maybe the download went wrong?");
        }

        logMessage = "\tedge_types... ";
        System.out.print(logMessage);
        sqlFile = new File(TEMP_CSV_FOLDER + File.separator + "relationTypes.csv");
        if (sqlFile.exists()) {
            query = "load data local infile '" + TEMP_CSV_FOLDER + "/relationTypes.csv" + "' " +
                    "into table edge_types " +
                    "fields " +
                    "terminated by '|' " +
                    "IGNORE 1 LINES " +
                    "(id,name,extendedName,info)";
            if (hasPassword) {
                processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-p\"" + PASSWORD + "\"", "-h", HOST, "-P", PORT, "--local-infile", DB, "-e", query);
            } else {
                processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-h", HOST, "-P", PORT, "--local-infile", DB, "-e", query);
            }
            try {
                if (LOG_MYSQL) {
                    append(logMessage + System.lineSeparator(), mysqlErrorLog);
                    append(logMessage + System.lineSeparator(), mysqlOutputLog);
                    processBuilder.redirectError(Redirect.appendTo(mysqlErrorLog));
                    processBuilder.redirectOutput(Redirect.appendTo(mysqlOutputLog));
                }
                processBuilder.start().waitFor();
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("done!");
        } else {
            System.out.println("Skipped edge_types import because \"" + sqlFile.getAbsolutePath() + "\" is missing... maybe the download went wrong?");
        }

        //Remove autocommit
        logMessage = "\tRemoving autocommit to speed up the process... ";
        System.out.print(logMessage);
        query = "SET GLOBAL AUTOCOMMIT = 0;";
        if (hasPassword) {
            processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-p\"" + PASSWORD + "\"", "-h", HOST, "-P", PORT, DB, "-e", query);
        } else {
            processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-h", HOST, "-P", PORT, DB, "-e", query);
        }
        try {
            if (LOG_MYSQL) {
                append(logMessage + System.lineSeparator(), mysqlErrorLog);
                append(logMessage + System.lineSeparator(), mysqlOutputLog);
                processBuilder.redirectError(Redirect.appendTo(mysqlErrorLog));
                processBuilder.redirectOutput(Redirect.appendTo(mysqlOutputLog));
            }
            processBuilder.start().waitFor();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.out.println("done!");


        //Count node and edge parts
        tempFolder = new File(TEMP_CSV_FOLDER);
        nodeParts = edgeParts = 0;
        for (File subFile : tempFolder.listFiles()) {
            if (subFile.getName().startsWith("nodes_")) {
                ++nodeParts;
            } else if (subFile.getName().startsWith("relations_")) {
                ++edgeParts;
            }
        }


        System.out.println("\tnodes... ");
        basepathCsvFile = TEMP_CSV_FOLDER + File.separator + "nodes_";
        part = 1;
        sqlFile = new File(basepathCsvFile + String.valueOf(part) + ".csv");
        if (!sqlFile.exists()) {
            System.out.println("Skipped nodes import because \"" + sqlFile.getAbsolutePath() + "\" is missing... maybe the download went wrong?");
        }
        while (sqlFile.exists()) {
            query = "load data local infile '" + TEMP_CSV_FOLDER + "/nodes_" + String.valueOf(part) + ".csv" + "' " +
                    "into table nodes " +
                    "fields " +
                    "terminated by '|' " +
                    "IGNORE 1 LINES " +
                    "(id,name,type,weight);commit;";
            logMessage = "\t\tpart#" + part + "/" + nodeParts + "... ";
            System.out.print(logMessage);
            importTimer = System.nanoTime();
            if (hasPassword) {
                processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-p\"" + PASSWORD + "\"", "-h", HOST, "-P", PORT, "--local-infile", DB, "-e", query);
            } else {
                processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-h", HOST, "-P", PORT, "--local-infile", DB, "-e", query);
            }
            try {
                if (LOG_MYSQL) {
                    append(logMessage + System.lineSeparator(), mysqlErrorLog);
                    append(logMessage + System.lineSeparator(), mysqlOutputLog);
                    processBuilder.redirectError(Redirect.appendTo(mysqlErrorLog));
                    processBuilder.redirectOutput(Redirect.appendTo(mysqlOutputLog));
                }
                processBuilder.start().waitFor();
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
            importTimer = (System.nanoTime() - importTimer) / 1_000_000;
            System.out.println("done in " + format.format(importTimer) + " ms.");
            ++part;
            sqlFile = new File(basepathCsvFile + String.valueOf(part) + ".csv");
        }


        System.out.println("\tedges (this may take a little while, grab a coffee)... ");


        basepathCsvFile = TEMP_CSV_FOLDER + File.separator + "relations_";
        part = 1;
        sqlFile = new File(basepathCsvFile + String.valueOf(part) + ".csv");
        if (!sqlFile.exists()) {
            System.out.println("Skipped edges import because \"" + sqlFile.getAbsolutePath() + "\" is missing... maybe the download went wrong?");
        }

        while (sqlFile.exists()) {
            logMessage = "\t\tpart#" + part + "/" + edgeParts + "... ";
            System.out.print(logMessage);
            importTimer = System.nanoTime();
            query = "load data local infile '" + TEMP_CSV_FOLDER + "/relations_" + String.valueOf(part) + ".csv" + "' " +
                    "into table edges " +
                    "fields " +
                    "terminated by '|' " +
                    "IGNORE 1 LINES " +
                    "(id,source,destination,type,weight);commit;";
            if (hasPassword) {
                processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-p\"" + PASSWORD + "\"", "-h", HOST, "-P", PORT, "--local-infile", DB, "-e", query);
            } else {
                processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-h", HOST, "-P", PORT, "--local-infile", DB, "-e", query);
            }
            try {
                if (LOG_MYSQL) {
                    append(logMessage + System.lineSeparator(), mysqlErrorLog);
                    append(logMessage + System.lineSeparator(), mysqlOutputLog);
                    processBuilder.redirectError(Redirect.appendTo(mysqlErrorLog));
                    processBuilder.redirectOutput(Redirect.appendTo(mysqlOutputLog));
                }
                processBuilder.start().waitFor();
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
            importTimer = (System.nanoTime() - importTimer) / 1_000_000;
            System.out.println("done in " + format.format(importTimer) + " ms.");
            ++part;
            sqlFile = new File(basepathCsvFile + String.valueOf(part) + ".csv");
        }

        if (!localInfileValue) {
            logMessage = "Resetting local_infile value as 'false'";
            System.out.println(logMessage);
            query = "set global local_infile=0;";
            if (hasPassword) {
                processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-p\"" + PASSWORD + "\"", "-h", HOST, "-P", PORT, "-e", query);
            } else {
                processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-h", HOST, "-P", PORT, "-e", query);
            }
            try {
                if (LOG_MYSQL) {
                    append(logMessage + System.lineSeparator(), mysqlErrorLog);
                    append(logMessage + System.lineSeparator(), mysqlOutputLog);
                    processBuilder.redirectError(Redirect.appendTo(mysqlErrorLog));
                    processBuilder.redirectOutput(Redirect.appendTo(mysqlOutputLog));
                }
                processBuilder.start().waitFor();
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }

        //Add autocommit
        logMessage = "Adding autocommit... ";
        System.out.print(logMessage);
        query = "SET GLOBAL AUTOCOMMIT = 1;";
        if (hasPassword) {
            processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-p\"" + PASSWORD + "\"", "-h", HOST, "-P", PORT, DB, "-e", query);
        } else {
            processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-h", HOST, "-P", PORT, DB, "-e", query);
        }
        try {
            if (LOG_MYSQL) {
                append(logMessage + System.lineSeparator(), mysqlErrorLog);
                append(logMessage + System.lineSeparator(), mysqlOutputLog);
                processBuilder.redirectError(Redirect.appendTo(mysqlErrorLog));
                processBuilder.redirectOutput(Redirect.appendTo(mysqlOutputLog));
            }
            processBuilder.start().waitFor();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.out.println("done!");

        //Add unique checks
        logMessage = "Adding unique checks... ";
        System.out.print(logMessage);
        query = "SET GLOBAL unique_checks = 1;";
        if (hasPassword) {
            processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-p\"" + PASSWORD + "\"", "-h", HOST, "-P", PORT, DB, "-e", query);
        } else {
            processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-h", HOST, "-P", PORT, DB, "-e", query);
        }
        try {
            if (LOG_MYSQL) {
                append(logMessage + System.lineSeparator(), mysqlErrorLog);
                append(logMessage + System.lineSeparator(), mysqlOutputLog);
                processBuilder.redirectError(Redirect.appendTo(mysqlErrorLog));
                processBuilder.redirectOutput(Redirect.appendTo(mysqlOutputLog));
            }
            processBuilder.start().waitFor();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.out.println("done!");

        //Add foreign key checks
        logMessage = "Adding foreign key checks... ";
        System.out.print(logMessage);
        query = "SET GLOBAL foreign_key_checks = 1;";
        if (hasPassword) {
            processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-p\"" + PASSWORD + "\"", "-h", HOST, "-P", PORT, DB, "-e", query);
        } else {
            processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-h", HOST, "-P", PORT, DB, "-e", query);
        }
        try {
            if (LOG_MYSQL) {
                append(logMessage + System.lineSeparator(), mysqlErrorLog);
                append(logMessage + System.lineSeparator(), mysqlOutputLog);
                processBuilder.redirectError(Redirect.appendTo(mysqlErrorLog));
                processBuilder.redirectOutput(Redirect.appendTo(mysqlOutputLog));
            }
            processBuilder.start().waitFor();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.out.println("done!");


        //TABLES UPDATE
        sqlFile = new File(UPDATE_FILEPATH);
        if (sqlFile.exists()) {
            logMessage = "Updating tables, adding index from file \"" + sqlFile.getName() + "\"... ";
            System.out.print(logMessage + " (this may take a little while, grab a coffee or two)");
            query = "source " + sqlFile.getAbsolutePath();
            if (hasPassword) {
                processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-p\"" + PASSWORD + "\"", "-h", HOST, "-P", PORT, DB, "-e", query);
            } else {
                processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-h", HOST, "-P", PORT, DB, "-e", query);
            }
            try {
                if (LOG_MYSQL) {
                    append(logMessage + System.lineSeparator(), mysqlErrorLog);
                    append(logMessage + System.lineSeparator(), mysqlOutputLog);
                    processBuilder.redirectError(Redirect.appendTo(mysqlErrorLog));
                    processBuilder.redirectOutput(Redirect.appendTo(mysqlOutputLog));
                }
                processBuilder.start().waitFor();
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
                System.exit(1);
            }
            System.out.println("done!");
        } else {
            System.out.println("Skipped Table updating because \"" + sqlFile.getAbsolutePath() + "\" is missing...");
        }

        if (!sqlModes.contains("NO_AUTO_VALUE_ON_ZERO")) {
            logMessage = "Removing 'NO_AUTO_VALUE_ON_ZERO' in sql_mode";
            System.out.println(logMessage);
            query = "set global sql_mode='" + sqlModes + "';";
            if (hasPassword) {
                processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-p\"" + PASSWORD + "\"", "-h", HOST, "-P", PORT, "-e", query);
            } else {
                processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-h", HOST, "-P", PORT, "-e", query);
            }
            try {
                if (LOG_MYSQL) {
                    append(logMessage + System.lineSeparator(), mysqlErrorLog);
                    append(logMessage + System.lineSeparator(), mysqlOutputLog);
                    processBuilder.redirectError(Redirect.appendTo(mysqlErrorLog));
                    processBuilder.redirectOutput(Redirect.appendTo(mysqlOutputLog));
                }
                processBuilder.start().waitFor();
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }


        if (CLEAN_AFTER) {
            System.out.print("Cleaning temporary files... ");
            deleteTemporary(tempFolder);
            System.out.println("done!");
        }

        timer = System.currentTimeMillis() - timer;
        System.out.println("Finished in " + format.format(timer / 1_000) + " sec.");
    }


    public static void deleteTemporary(File temporary) {
        if (temporary.isFile()) {
            temporary.delete();
        } else {
            for (File file : temporary.listFiles()) {
                deleteTemporary(file);
            }
            temporary.delete();
        }
    }


    public static void argsProcess(String[] args) {
        int index = 0;
        int length = args.length;
        String arg;

        while (index < length) {
            arg = args[index++];
            if (arg.equals("-h") || arg.equals("--help")) {
                usage();
            } else if (arg.equals("-d") || arg.equals("--database")) {
                if (index < length) {
                    DB = args[index++];
                } else {
                    System.err.println("No value for found for argument \"" + arg + "\", using default (" + DB + ")");
                }
            } else if (arg.equals("-u") || arg.equals("--username")) {
                if (index < length) {
                    USERNAME = args[index++];
                } else {
                    System.err.println("No value for found for argument \"" + arg + "\", using default (" + USERNAME + ")");
                }
            } else if (arg.equals("-p") || arg.equals("--password")) {
                if (index < length) {
                    PASSWORD = args[index++];
                } else {
                    System.err.println("No value for found for argument \"" + arg + "\", using default (" + PASSWORD + ")");
                }
            } else if (arg.equals("-H") || arg.equals("--host")) {
                if (index < length) {
                    HOST = args[index++];
                } else {
                    System.err.println("No value for found for argument \"" + arg + "\", using default (" + HOST + ")");
                }
            } else if (arg.equals("-P") || arg.equals("--port")) {
                if (index < length) {
                    PORT = args[index++];
                } else {
                    System.err.println("No value for found for argument \"" + arg + "\", using default (" + PORT + ")");
                }
            } else if (arg.equals("-i") || arg.equals("--init")) {
                if (index < length) {
                    INIT_FILEPATH = args[index++];
                } else {
                    System.err.println("No value for found for argument \"" + arg + "\", using default (" + INIT_FILEPATH + ")");
                }
            } else if (arg.equals("-U") || arg.equals("--update")) {
                if (index < length) {
                    UPDATE_FILEPATH = args[index++];
                } else {
                    System.err.println("No value for found for argument \"" + arg + "\", using default (" + UPDATE_FILEPATH + ")");
                }
            } else if (arg.equals("-t") || arg.equals("--temp")) {
                if (index < length) {
                    TEMP_CSV_FOLDER = args[index++];
                } else {
                    System.err.println("No value for found for argument \"" + arg + "\", using default (" + TEMP_CSV_FOLDER + ")");
                }
            } else if (arg.equals("-s") || arg.equals("--size")) {
                if (index < length) {
                    PARTITIONS_SIZE = Integer.parseInt(args[index++]);
                } else {
                    System.err.println("No value for found for argument \"" + arg + "\", using default (" + PARTITIONS_SIZE + ")");
                }
            }
            //FLAGS
            else if (arg.equals("--no-download")) {
                DOWNLOAD_LAST_DUMP = false;
            } else if (arg.equals("--drop")) {
                DROP_IF_EXIST = true;
            } else if (arg.equals("--keep")) {
                CLEAN_AFTER = false;
            } else if (arg.equals("--log")) {
                LOG_MYSQL = true;
            }
        }
    }


    private static void append(String message, File file) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file, true))) {
            writer.write(message);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private static void usage() {
        System.out.println("MySQL Import tool for rezoJDM. "
                + "The program will fetch the last avalaible dump (in zip format, 1+GB) from jeuxdemots "
                + "(http://www.jeuxdemots.org/JDM-LEXICALNET-FR/) "
                + "and import it into a new MySQL database.");
        System.out.println("MySQL must be installed and added to the system PATH variable.");

        System.out.println("There is no mandatory argument but you might need (or just want) to set some of them.");
        System.out.println();
        System.out.println("MySQL related parameters");
        System.out.println("\t-d/--database [DATABASE_NAME]: Database name (DEFAULT=\"" + DB + "\")");
        System.out.println("\t-u/--username [USERNAME]: MySQL username (DEFAULT=\"" + USERNAME + "\")");
        System.out.println("\t-p/--password [PASSWORD]: MySQL password, leave empty if there is no password (DEFAULT=\"" + PASSWORD + "\")");
        System.out.println("\t--drop: Drop previous database with the same name (DEFAULT=\"" + String.valueOf(DROP_IF_EXIST) + "\")");
        System.out.println();
        System.out.println("Other parameters: ");
        System.out.println("\t--log: Create logs for ouputs and errors from MySQL queries (DEFAULT=\"" + String.valueOf(LOG_MYSQL) + "\")");
        System.out.println("\t-i/--init [INIT_FILEPATH]: Filepath of the sql init file (DEFAULT=\"" + INIT_FILEPATH + "\")");
        System.out.println("\t-u/--update [UPDATE_FILEPATH]: Filepath of the sql update file (DEFAULT=\"" + UPDATE_FILEPATH + "\")");
        System.out.println("\t-t/--temp [TEMPORARY_DOWNLOAD_DIRPATH]: Filepath of the temporary directory storing the dump and the csv files (DEFAULT=\"" + TEMP_CSV_FOLDER + "\")");
        System.out.println("\t--keep: Do not delete the temporary folder and all its content before exiting "
                + "(DEFAULT=\"" + String.valueOf(!CLEAN_AFTER) + "\")");
        System.out.println("\t--no-download: Do not attempt to download the lastest dump and instead "
                + "try to read existing file from the temporary folder (DEFAULT=\"" + String.valueOf(!DOWNLOAD_LAST_DUMP) + "\")");
        System.out.println("\t-s/--size [PARTITION_SIZE]: Number of elements in each subfiles use to import nodes and edges. "
                + "A powerfull machine might not need to split the csv files but in most cases, the entire dump cannot be imported all at once. "
                + "Be careful as using a value too low might create a lot of files."
                + "Use 0 to not split any files (DEFAULT=" + PARTITIONS_SIZE + ")");
        System.exit(1);
    }
}
