package tools;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;

public class CreateAndLoad {

	private static String DB = "rezoJDM"; //-d or --database
	private static String USERNAME = "root"; //-u or --username
	private static String PASSWORD = ""; //-p or --password 


	private static boolean DOWNLOAD_LAST_DUMP = true; //--no-download (flag)
	private static boolean DROP_IF_EXIST = false; // --drop (flag)
	private static boolean CLEAN_AFTER = true; // --keep (flag)
	private static String INIT_FILEPATH = "init.sql"; //-i or --init 
	private static String TEMP_CSV_FOLDER = "__tmpRezoJDMCSV"; //-t or --temp



	public static void main(String[] args) {
		argsProcess(args);		
		boolean hasPassword = !PASSWORD.isEmpty();

		long timer;
		DecimalFormat format = new DecimalFormat();
		ProcessBuilder processBuilder;
		String query, basepathCsvFile;
		int part;
		timer = System.currentTimeMillis();
		//DL
		if(DOWNLOAD_LAST_DUMP) {
			System.out.println("Downloading dump and converting it into CSV files (this may take a few minutes)... ");
			DownloadAndConvert.downloadAndCSVConvert(TEMP_CSV_FOLDER, CLEAN_AFTER);
		}else {
			System.out.println("Skipping dump download...");			
		}

		//DROP DB
		if(DROP_IF_EXIST) {
			System.out.print("Dropping previous database (if it exists)... ");
			query = "\"DROP DATABASE IF EXISTS "+DB+"\"";
			if(hasPassword) {			
				processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-p\""+PASSWORD+"\"", "-e", query);
			}else {
				processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-e", query);			
			}
			try {			
				//processBuilder.redirectError(new File("error.log"));
				//processBuilder.redirectOutput(new File("output.log"));
				processBuilder.start().waitFor();					
			} catch (IOException | InterruptedException e) {
				e.printStackTrace();
			}
			System.out.println("done!");
		}


		//CREATE DB
		System.out.print("Creating if not exists db=\""+DB+"\"... ");		
		query = "\"CREATE DATABASE IF NOT EXISTS "+ DB +" CHARACTER SET='utf8' COLLATE='utf8_bin';\"";		
		if(hasPassword) {	
			processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-p\""+PASSWORD+"\"", "-e", query);
		}else {
			processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-e", query);			
		}
		try {	
			//			processBuilder.redirectError(new File("error.log"));
			//			processBuilder.redirectOutput(new File("output.log"));
			System.out.println(processBuilder.command().toString());
			processBuilder.start().waitFor();					
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
			System.exit(1);
		}
		System.out.println("done!");

		//TABLES INITIALISATION 
		File sqlFile = new File(INIT_FILEPATH);
		if(sqlFile.exists()) {
			System.out.print("Tables initialisation from file \""+sqlFile.getName()+"\"... ");
			query = "source "+sqlFile.getAbsolutePath();
			if(hasPassword) {
				processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-p\""+PASSWORD+"\"", DB, "-e", query);
			}else {
				processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, DB, "-e", query);				
			}
			try {
				processBuilder.start().waitFor();					
			} catch (IOException | InterruptedException e) {
				e.printStackTrace();
				System.exit(1);
			}
			System.out.println("done!");
		}else {
			System.out.println("Skipped Table initialisation because \""+sqlFile.getAbsolutePath()+"\" is missing...");
		}

		System.out.println("Importing data: ");


		System.out.print("\tnode_types... ");
		sqlFile = new File(TEMP_CSV_FOLDER + File.separator + "nodeTypes.csv");
		if(sqlFile.exists()) {			
			query = "\"load data local infile '"+TEMP_CSV_FOLDER + "/nodeTypes.csv"+"' " + 
					"into table node_types " + 
					"fields " +
					"terminated by '|' " +					
					"IGNORE 1 LINES " +
					"(id,name,info);\"";		
			if(hasPassword){	
				processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-p\""+PASSWORD+"\"", "--local-infile", DB, "-e", query);
			}else{
				processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "--local-infile", DB, "-e", query);				
			}
			try {
				processBuilder.start().waitFor();					
			} catch (IOException | InterruptedException e) {
				e.printStackTrace();
			}
			System.out.println("done!");
		}else {
			System.out.println("Skipped node_types import because \""+sqlFile.getAbsolutePath()+"\" is missing... maybe the download went wrong?");
		}

		System.out.print("\tedge_types... ");
		sqlFile = new File(TEMP_CSV_FOLDER + File.separator + "relationTypes.csv");
		if(sqlFile.exists()) {			
			query = "\"load data local infile '"+TEMP_CSV_FOLDER + "/relationTypes.csv"+"' " + 
					"into table edge_types " + 
					"fields " +
					"terminated by '|' " +					
					"IGNORE 1 LINES " +
					"(id,name,extendedName,info);\"";		
			if(hasPassword){				
				processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-p\""+PASSWORD+"\"", "--local-infile", DB, "-e", query);
			}else{			
				processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "--local-infile", DB, "-e", query);				
			}
			try {
				processBuilder.start().waitFor();					
			} catch (IOException | InterruptedException e) {
				e.printStackTrace();
			}
			System.out.println("done!");
		}else {
			System.out.println("Skipped edge_types import because \""+sqlFile.getAbsolutePath()+"\" is missing... maybe the download went wrong?");
		}

		System.out.println("\tnodes (this may take a little while, maybe grab a coffee)... ");
		basepathCsvFile = TEMP_CSV_FOLDER + File.separator + "nodes_";
		part = 1;	
		sqlFile = new File(basepathCsvFile + String.valueOf(part) + ".csv");
		if(!sqlFile.exists()) {
			System.out.println("Skipped nodes import because \""+sqlFile.getAbsolutePath()+"\" is missing... maybe the download went wrong?");
		}
		while(sqlFile.exists()) {
			query = "\"load data local infile '"+TEMP_CSV_FOLDER + "/nodes_"+String.valueOf(part)+".csv"+"' " + 
					"into table nodes " + 
					"fields " +
					"terminated by '|' " +					
					"IGNORE 1 LINES " +
					"(id,name,type,weight);\"";		
			System.out.print("\tpart#"+part+"... ");
			if(hasPassword){				
				processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-p\""+PASSWORD+"\"", "--local-infile", DB, "-e", query);
			}else{			
				processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "--local-infile", DB, "-e", query);
			}
			try {
				processBuilder.start().waitFor();					
			} catch (IOException | InterruptedException e) {
				e.printStackTrace();
			}
			System.out.println("done.");
			++part;
			sqlFile = new File(basepathCsvFile + String.valueOf(part) + ".csv");
		}


		System.out.println("\tedges (this may take a little while, grab a coffee or two)... ");
		basepathCsvFile = TEMP_CSV_FOLDER + File.separator + "relations_";
		part = 1;	
		sqlFile = new File(basepathCsvFile + String.valueOf(part) + ".csv");
		if(!sqlFile.exists()) {
			System.out.println("Skipped edges import because \""+sqlFile.getAbsolutePath()+"\" is missing... maybe the download went wrong?");
		}

		while(sqlFile.exists()) {
			System.out.print("\tpart#"+part+"... ");
			query = "\"load data local infile '"+TEMP_CSV_FOLDER + "/relations_"+String.valueOf(part)+".csv"+"' " + 
					"into table edges " + 
					"fields " +
					"terminated by '|' " +					
					"IGNORE 1 LINES " +
					"(id,source,destination,type,weight);\"";		
			if(hasPassword){				
				processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "-p\""+PASSWORD+"\"", "--local-infile", DB, "-e", query);
			}else{				
				processBuilder = new ProcessBuilder("mysql", "-u", USERNAME, "--local-infile", DB, "-e", query);				
			}
			try {
//				processBuilder.redirectError(new File("error_"+String.valueOf(part)+".log"));
//				processBuilder.redirectOutput(new File("output_"+String.valueOf(part)+".log"));
				processBuilder.start().waitFor();					
			} catch (IOException | InterruptedException e) {
				e.printStackTrace();
			}
			System.out.println("done.");
			++part;
			sqlFile = new File(basepathCsvFile + String.valueOf(part) + ".csv");
		}

		if(CLEAN_AFTER) {
			System.out.print("Cleaning temporary files... ");
			File tempFolder = new File(TEMP_CSV_FOLDER);
			deleteTemporary(tempFolder);			
			System.out.println("done!");
		}

		timer = System.currentTimeMillis() - timer;
		System.out.println("Finished in "+format.format(timer / 1_000)+ " sec.");
	}

	public static void deleteTemporary(File temporary) {
		if(temporary.isFile()) {
			temporary.delete();
		}else {
			for(File file : temporary.listFiles()) {
				deleteTemporary(file);
			}
			temporary.delete();
		}
	}	


	public static void argsProcess(String[] args) {
		int index = 0;
		int length = args.length;
		String arg;

		while(index < length) {
			arg = args[index++].toLowerCase();
			if(arg.equals("-h") || arg.equals("--help")) {
				usage();
			}
			else if(arg.equals("-d") || arg.equals("--database")) {
				if(index < length) {
					DB = args[index++];
				}else {
					System.err.println("No value for found for argument \""+arg+"\", using default ("+DB+")");
				}
			}
			else if(arg.equals("-u") || arg.equals("--username")) {
				if(index < length) {
					USERNAME = args[index++];
				}else {
					System.err.println("No value for found for argument \""+arg+"\", using default ("+USERNAME+")");
				}
			}
			else if(arg.equals("-p") || arg.equals("--password")) {
				if(index < length) {
					PASSWORD = args[index++];
				}else {
					System.err.println("No value for found for argument \""+arg+"\", using default ("+PASSWORD+")");
				}
			}
			else if(arg.equals("-i") || arg.equals("--init")) {
				if(index < length) {
					INIT_FILEPATH = args[index++];
				}else {
					System.err.println("No value for found for argument \""+arg+"\", using default ("+INIT_FILEPATH+")");
				}
			}
			else if(arg.equals("-t") || arg.equals("--temp")) {
				if(index < length) {
					TEMP_CSV_FOLDER = args[index++];
				}else {
					System.err.println("No value for found for argument \""+arg+"\", using default ("+TEMP_CSV_FOLDER+")");
				}
			}
			//FLAGS
			else if(arg.equals("--no-download")) {
				DOWNLOAD_LAST_DUMP = false;
			}else if(arg.equals("--drop")) {
				DROP_IF_EXIST = true;
			}else if(arg.equals("--keep")) {
				CLEAN_AFTER = false;
			}
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
		System.out.println("\t-d/--database [DATABASE_NAME]: Database name (DEFAULT=\""+DB+"\")");
		System.out.println("\t-u/--username [USERNAME]: MySQL username (DEFAULT=\""+USERNAME+"\")");
		System.out.println("\t-p/--password [PASSWORD]: MySQL password, leave empty if there is no password (DEFAULT=\""+PASSWORD+"\")");
		System.out.println("\t-u/--username [USERNAME]: MySQL username (DEFAULT=\""+USERNAME+"\")");
		System.out.println("\t--drop: Drop previous database with the same name (DEFAULT=\""+String.valueOf(DROP_IF_EXIST)+"\")");
		System.out.println();
		System.out.println("Other parameters: ");
		System.out.println("\t-i/--init [INIT_FILEPATH]: Filepath of the sql init file (DEFAULT=\""+INIT_FILEPATH+"\")");
		System.out.println("\t-t/--temp [TEMPORARY_DOWNLOAD_DIRPATH]: Filepath of the temporary directory storing the dump and the csv files (DEFAULT=\""+TEMP_CSV_FOLDER+"\")");
		System.out.println("\t--keep: Do not delete the temporary folder and all its content before exiting "
				+ "(DEFAULT=\""+String.valueOf(!CLEAN_AFTER)+"\")");
		System.out.println("\t--no-download: Do not attempt to download the lastest dump and instead "
				+ "try to read existing file from the temporary folder (DEFAULT=\""+String.valueOf(!DOWNLOAD_LAST_DUMP)+"\")");

		System.exit(1);
	}
}
