package tools;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;

public class CreateAndLoad {

	private static final String DB_DEFAULT = "rezoJDM";
	private static final String USERNAME_DEFAULT = "root";
	private static final String PASSWORD_DEFAULT = "";


	private static final boolean DOWNLOAD_LAST_DUMP_DEFAULT = true;
	private static final boolean DROP_IF_EXIST_DEFAULT = false;
	private static final boolean CLEAN_AFTER_DEFAULT = true;
	private static final String INIT_FILEPATH_DEFAULT = "init.sql";
	private static final String TEMP_CSV_FOLDER_DEFAULT = "__tmpRezoJDMCSV";





	public static void main(String[] args) {		
		String db = DB_DEFAULT;
		String username = USERNAME_DEFAULT;
		String password = PASSWORD_DEFAULT;		

		boolean downloadLastDump = DOWNLOAD_LAST_DUMP_DEFAULT;
		boolean dropIfExist = DROP_IF_EXIST_DEFAULT;
		boolean cleanAfter = CLEAN_AFTER_DEFAULT;
		String initFilepath = INIT_FILEPATH_DEFAULT;
		String tempCSVFolder = TEMP_CSV_FOLDER_DEFAULT; 

		String arg;
		if(args.length>0) {
			arg = args[0];
			if(arg.startsWith("-h") || arg.startsWith("--h")) {
				usage();
			}else {
				db = arg;
				if(args.length > 1) {
					username = args[1];
					if(args.length > 2) {
						password = args[2];
					}
				}
			}			
		}

		boolean hasPassword = !password.isEmpty();


		long timer;
		DecimalFormat format = new DecimalFormat();
		ProcessBuilder processBuilder;
		String query; 
		timer = System.currentTimeMillis();
		//DL
		if(downloadLastDump) {
			System.out.println("Downloading dump and converting it into CSV files (this may take a few minutes)... ");
			DownloadAndConvert.downloadAndCSVConvert(TEMP_CSV_FOLDER_DEFAULT);
		}else {
			System.out.println("Skipping dump download...");			
		}

		//DROP DB
		if(dropIfExist) {
			System.out.print("Dropping previous database (if it exists)... ");
			query = "\"DROP DATABASE IF EXISTS "+db+"\"";
			if(hasPassword) {			
				processBuilder = new ProcessBuilder("mysql", "-u", username, "-p", password, "-e", query);
			}else {
				processBuilder = new ProcessBuilder("mysql", "-u", username, "-e", query);			
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
		System.out.print("Creating if not exists db=\""+db+"\"... ");		
		query = "\"CREATE DATABASE IF NOT EXISTS "+ db +" CHARACTER SET='utf8' COLLATE='utf8_bin';\"";		
		if(hasPassword) {			
			processBuilder = new ProcessBuilder("mysql", "-u", username, "-p", password, "-e", query);
		}else {
			processBuilder = new ProcessBuilder("mysql", "-u", username, "-e", query);			
		}
		try {		
			processBuilder.start().waitFor();					
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
			System.exit(1);
		}
		System.out.println("done!");

		//TABLES INITIALISATION 
		File sqlFile = new File(initFilepath);
		if(sqlFile.exists()) {
			System.out.print("Tables initialisation from file \""+sqlFile.getName()+"\"... ");
			query = "source "+sqlFile.getAbsolutePath();
			if(hasPassword) {
				processBuilder = new ProcessBuilder("mysql", "-u", username, "-p", password, db, "-e", query);
			}else {
				processBuilder = new ProcessBuilder("mysql", "-u", username, db, "-e", query);				
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
		sqlFile = new File(tempCSVFolder + File.separator + "nodeTypes.csv");
		if(sqlFile.exists()) {			
			query = "\"load data local infile '"+tempCSVFolder + "/nodeTypes.csv"+"' " + 
					"into table node_types " + 
					"fields " +
					"terminated by '|' " +					
					"IGNORE 1 LINES " +
					"(id,name,info);\"";		
			if(password.isEmpty()){
				processBuilder = new ProcessBuilder("mysql", "-u", username, "--local-infile", db, "-e", query);
			}else{
				processBuilder = new ProcessBuilder("mysql", "-u", username, "-p", password, "--local-infile", db, "-e", query);
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
		sqlFile = new File(tempCSVFolder + File.separator + "relationTypes.csv");
		if(sqlFile.exists()) {			
			query = "\"load data local infile '"+tempCSVFolder + "/relationTypes.csv"+"' " + 
					"into table edge_types " + 
					"fields " +
					"terminated by '|' " +					
					"IGNORE 1 LINES " +
					"(id,name,extendedName,info);\"";		
			if(hasPassword){				
				processBuilder = new ProcessBuilder("mysql", "-u", username, "-p", password, "--local-infile", db, "-e", query);
			}else{			
				processBuilder = new ProcessBuilder("mysql", "-u", username, "--local-infile", db, "-e", query);				
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

		System.out.print("\tnodes (this may take a little while, maybe grab a coffee)... ");
		sqlFile = new File(tempCSVFolder + File.separator + "nodes.csv");
		if(sqlFile.exists()) {			
			query = "\"load data local infile '"+tempCSVFolder + "/nodes.csv"+"' " + 
					"into table nodes " + 
					"fields " +
					"terminated by '|' " +					
					"IGNORE 1 LINES " +
					"(id,name,type,weight);\"";		
			if(hasPassword){				
				processBuilder = new ProcessBuilder("mysql", "-u", username, "-p", password, "--local-infile", db, "-e", query);
			}else{			
				processBuilder = new ProcessBuilder("mysql", "-u", username, "--local-infile", db, "-e", query);
			}
			try {
				processBuilder.start().waitFor();					
			} catch (IOException | InterruptedException e) {
				e.printStackTrace();
			}
			System.out.println("done!");
		}else {
			System.out.println("Skipped nodes import because \""+sqlFile.getAbsolutePath()+"\" is missing... maybe the download went wrong?");
		}

		System.out.print("\tedges (this may take a little while, grab a coffee or two)... ");
		sqlFile = new File(tempCSVFolder + File.separator + "relations.csv");
		if(sqlFile.exists()) {			
			query = "\"load data local infile '"+tempCSVFolder + "/relations.csv"+"' " + 
					"into table edges " + 
					"fields " +
					"terminated by '|' " +					
					"IGNORE 1 LINES " +
					"(id,source,destination,type,weight);\"";		
			if(hasPassword){				
				processBuilder = new ProcessBuilder("mysql", "-u", username, "-p", password, "--local-infile", db, "-e", query);
			}else{				
				processBuilder = new ProcessBuilder("mysql", "-u", username, "--local-infile", db, "-e", query);				
			}
			try {
				processBuilder.start().waitFor();					
			} catch (IOException | InterruptedException e) {
				e.printStackTrace();
			}
			System.out.println("done!");
		}else {
			System.out.println("Skipped edges import because \""+sqlFile.getAbsolutePath()+"\" is missing... maybe the download went wrong?");
		}


		if(cleanAfter) {
			System.out.print("Cleaning temporary files... ");
			File tempFolder = new File(tempCSVFolder);
			deleteTemporary(tempFolder);			
			System.out.println("done!");
		}

		timer = System.currentTimeMillis() - timer;
		System.out.println("Finished in "+format.format(timer / 1_000)+ " sec.");
	}

	private static void deleteTemporary(File temporary) {
		if(temporary.isFile()) {
			temporary.delete();
		}else {
			for(File file : temporary.listFiles()) {
				deleteTemporary(file);
			}
			temporary.delete();
		}
	}	

	private static void usage() {
		System.out.println("Create and load rezoJDM. 3 parameters (in that order): ");
		System.out.println("1st parameter: Name of database (DEFAULT=\""+DB_DEFAULT+"\"");
		System.out.println("2nd parameter: username (DEFAULT=\""+USERNAME_DEFAULT+"\"");
		System.out.println("1st parameter: password (DEFAULT=\""+PASSWORD_DEFAULT+"\"");
		System.exit(1);
	}
}
