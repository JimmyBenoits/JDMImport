package tools;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;

public class CreateAndLoad {

	private static final String DEFAULT_DB = "rezoJDM";
	private static final String DEFAULT_USERNAME = "root";
	private static final String DEFAULT_PASSWORD = "";

	private static final String INIT_FILEPATH_DEFAULT = "init.sql";
	private static final String TEMP_CSV_FOLDER = "__tmpRezoJDMCSV";
	private static final boolean CLEAN_AFTER = true;


	public static void main(String[] args) {
		String db = DEFAULT_DB;
		String username = DEFAULT_USERNAME;
		String password = DEFAULT_PASSWORD;
		String arg;
		if(args.length>0) {
			arg = args[0];
			if(arg.startsWith("-h") || arg.startsWith("--h")) {
				System.out.println("Create and load rezoJDM. 3 parameters (in that order): ");
				System.out.println("1st parameter: Name of database (DEFAULT=\""+DEFAULT_DB+"\"");
				System.out.println("2nd parameter: username (DEFAULT=\""+DEFAULT_USERNAME+"\"");
				System.out.println("1st parameter: password (DEFAULT=\""+DEFAULT_PASSWORD+"\"");
				System.exit(1);
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
		long timer;
		DecimalFormat format = new DecimalFormat();
		
		timer = System.currentTimeMillis();
		//DL
//		System.out.println("Downloading dump and converting it into CSV files");
//		System.out.println("This operation may take a few minutes... ");
//		DownloadAndConvert.downloadAndCSVConvert(TEMP_CSV_FOLDER);
//		System.out.println("done!");

		//CREATE DB
		System.out.print("Creating if not exists db=\""+db+"\"... ");
		ProcessBuilder processBuilder;
		String query = "\"CREATE DATABASE IF NOT EXISTS "+ db +" CHARACTER SET='utf8' COLLATE='utf8_bin';\"";
		if(password.isEmpty()) {
			processBuilder = new ProcessBuilder("mysql", "-u", username, "-e", query);
		}else {
			processBuilder = new ProcessBuilder("mysql", "-u", username, "-p", password, "-e", query);
		}
		try {
			//			processBuilder.redirectError(new File("error.log"));
			//			processBuilder.redirectOutput(new File("output.log"));
			processBuilder.start().waitFor();					
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
			System.exit(1);
		}
		System.out.println("done!");

		//TABLES INITIALISATION 
		File sqlFile = new File(INIT_FILEPATH_DEFAULT);
		if(sqlFile.exists()) {
			System.out.print("Tables initialisation from file \""+sqlFile.getName()+"\"... ");
			query = "source "+sqlFile.getAbsolutePath();
			if(password.isEmpty()) {
				processBuilder = new ProcessBuilder("mysql", "-u", username, db, "-e", query);
			}else {
				processBuilder = new ProcessBuilder("mysql", "-u", username, "-p", password, db, "-e", query);
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
		sqlFile = new File(TEMP_CSV_FOLDER + File.separator + "relationTypes.csv");
		if(sqlFile.exists()) {			
			query = "\"load data local infile '"+TEMP_CSV_FOLDER + "/relationTypes.csv"+"' " + 
					"into table edge_types " + 
					"fields " +
					"terminated by '|' " +					
					"IGNORE 1 LINES " +
					"(id,name,extendedName,info);\"";		
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
			System.out.println("Skipped edge_types import because \""+sqlFile.getAbsolutePath()+"\" is missing... maybe the download went wrong?");
		}
		
		System.out.print("\tnodes (this may take a little while, maybe grab a coffee)... ");
		sqlFile = new File(TEMP_CSV_FOLDER + File.separator + "nodes.csv");
		if(sqlFile.exists()) {			
			query = "\"load data local infile '"+TEMP_CSV_FOLDER + "/nodes.csv"+"' " + 
					"into table nodes " + 
					"fields " +
					"terminated by '|' " +					
					"IGNORE 1 LINES " +
					"(id,name,type,weight);\"";		
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
			System.out.println("Skipped nodes import because \""+sqlFile.getAbsolutePath()+"\" is missing... maybe the download went wrong?");
		}
		
		System.out.print("\tedges (this may take a little while, grab a coffee or two)... ");
		sqlFile = new File(TEMP_CSV_FOLDER + File.separator + "relations.csv");
		if(sqlFile.exists()) {			
			query = "\"load data local infile '"+TEMP_CSV_FOLDER + "/relations.csv"+"' " + 
					"into table edges " + 
					"fields " +
					"terminated by '|' " +					
					"IGNORE 1 LINES " +
					"(id,source,destination,type,weight);\"";		
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
			System.out.println("Skipped edges import because \""+sqlFile.getAbsolutePath()+"\" is missing... maybe the download went wrong?");
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
}
