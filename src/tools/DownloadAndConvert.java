package tools;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipInputStream;

public class DownloadAndConvert {

	private static final String DEFAULT_FOLDER = "__tmpRezoJDMCSV";
	private static final boolean DEFAULT_CLEAN_INTERMEDIARY_FILES = false;
	private static final int DEFAULT_PART_SIZE = 100_000;


	public static void main(String[] args) throws IOException {
		downloadAndCSVConvert(DEFAULT_FOLDER, DEFAULT_CLEAN_INTERMEDIARY_FILES, DEFAULT_PART_SIZE);
		//		downloadAndCSVConvert("tmp", DEFAULT_CLEAN_INTERMEDIARY_FILES, DEFAULT_PART_SIZE);
	}

	/**
	 * URL to most recent dump rezoJDM
	 */
	private static final String LAST_OUTPUT_NOHTML = "JDM-LEXICALNET-FR/LAST_OUTPUT_NOHTML.txt";
	private static final String REZO_BASE_URL =  "http://www.jeuxdemots.org/";
	private static final DecimalFormat format = new DecimalFormat();


	private static final Pattern relationTypesPattern = Pattern.compile(
			"^rtid=(-?\\d+)\\|"
					+ "name=\"(.*)\"\\|"
					+ "nom_etendu=\"(.*)\"\\|"
					+ "info=\"(.*)\"$");
	private static final Pattern nodeTypesPattern = Pattern.compile(
			"^ntid=(-?\\d+)\\|"
					+ "name=\"(.*)\"\\|"			
					+ "info=\"(.*)\"$");

	private static final Pattern nodePattern = Pattern.compile(
			"^eid=(-?\\d+)\\|"
					+ "n=\"(.*)\"\\|"
					+ "t=(-?\\d+)\\|"
					+ "w=(-?\\d+)"
					+ "(\\|nf=\"(.*)\")?");			

	private static final Pattern relationPattern = Pattern.compile(
			"^rid=(-?\\d+)\\|"
					+ "n1=(-?\\d+)\\|"
					+ "n2=(-?\\d+)\\|"
					+ "t=(-?\\d+)\\|"
					+ "w=(-?\\d+)$");	


	public static boolean downloadAndCSVConvert(String outputDirpath, boolean cleanIntermediateFiles, int partSize) {	
		Matcher matcher;
		long timer = System.currentTimeMillis();			
		boolean res = false;
		File outputDir, csvFile, zipFile;
		String dumppath, basepathCsvFile;
		Set<Integer> nodeIDs;
		int part;
		BufferedWriter fragmentedOutput;
		Integer id, idSource, idDestination;
		if(partSize <= 0) {
			partSize = Integer.MAX_VALUE;
		}
		try {
			URL url = new URL(REZO_BASE_URL + LAST_OUTPUT_NOHTML);
			String line = null;			
			int cpt, cptError;
			try(BufferedReader in = new BufferedReader(new InputStreamReader(url.openStream(), "cp1252"))){
				line = in.readLine();//read last output path				
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			if(line != null) {
				outputDir = new File(outputDirpath);
				if(!outputDir.exists()) {
					outputDir.mkdir();
				}

				url = new URL(REZO_BASE_URL + line + ".zip");
				dumppath = outputDirpath + File.separator + "lastdump.zip";
				zipFile = new File(dumppath);
				if(!zipFile.exists()) {
					System.out.println("\t**Downloading \""+REZO_BASE_URL + line + ".zip\"**");											
					try {
						Files.copy(url.openStream(),Paths.get(dumppath));
					} catch (IOException e) {
						e.printStackTrace();
						return false;
					}
				}else {
					System.out.println("\t**\""+REZO_BASE_URL + line + ".zip\" already exists**");											
				}
				System.out.println("\t**Unzipping...**");
				unzip(dumppath, outputDirpath);
				if(cleanIntermediateFiles) {
					zipFile.delete();
				}

				try(BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(outputDirpath + File.separator + line), "cp1252"))){	
					//header
					System.out.println("\t**Skipping header...**");
					while((line = in.readLine())!=null && !line.startsWith("// ---- RELATION TYPES")) {}					
					//relation types
					csvFile = new File(outputDirpath + File.separator + "relationTypes.csv");
					cptError = 0;
					System.out.println("\t**Reading relation types and converting into \""+csvFile.getName()+"\"**");
					try (BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(csvFile), StandardCharsets.UTF_8))) {
						//header for relation types
						out.write("id|name|name_etendu|info");
						out.newLine();
						while((line = in.readLine())!=null && !line.startsWith("// ---- NODE TYPES")) {
							matcher = relationTypesPattern.matcher(line);
							if(matcher.matches()) {
								out.write(matcher.group(1)+"|"+
										matcher.group(2)+"|"+
										matcher.group(3)+"|"+
										matcher.group(4));
								out.newLine();
							}else if(!line.isEmpty()){
								++cptError;
								System.err.println("error parsing relationType#"+format.format(cptError)+": "+line);
							}
						}
					}

					//node types
					csvFile = new File(outputDirpath + File.separator + "nodeTypes.csv");
					cptError = 0;
					System.out.println("\t**Reading node types and converting into \""+csvFile.getName()+"\"**");
					try (BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(csvFile), StandardCharsets.UTF_8))) {
						//header for node types
						out.write("id|name|info");
						out.newLine();
						while((line = in.readLine())!=null && !line.startsWith("// -- NODES")) {
							matcher = nodeTypesPattern.matcher(line);
							if(matcher.matches()) {
								out.write(matcher.group(1)+"|"+
										matcher.group(2)+"|"+
										matcher.group(3));
								out.newLine();
							}else if(!line.isEmpty()){
								++cptError;
								System.err.println("error parsing nodeType#"+format.format(cptError)+": "+line);
							}
						}
					}	

					//nodes					
					nodeIDs = new HashSet<>();
					basepathCsvFile = outputDirpath + File.separator + "nodes_";
					part = 1;
					cpt = 0;		
					cptError = 0;					
					csvFile = new File(basepathCsvFile + String.valueOf(part) + ".csv");
					fragmentedOutput = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(csvFile), StandardCharsets.UTF_8)); 
					//					csvFile = new File(outputDirpath + File.separator + "nodes.csv");
					System.out.println("\t**Reading nodes and converting into \""+csvFile.getName()+"\"**");
					fragmentedOutput.write("id|name|type|weight");
					fragmentedOutput.newLine();
					while((line = in.readLine())!=null && !line.startsWith("// -- RELATIONS")) {						
						matcher = nodePattern.matcher(line);
						if(matcher.matches()) {
							id = Integer.parseInt(matcher.group(1));
							nodeIDs.add(id);							
							fragmentedOutput.write(matcher.group(1)+"|"+
									matcher.group(2)+"|"+
									matcher.group(3)+"|"+
									matcher.group(4));
							fragmentedOutput.newLine();
							++cpt;
							if(cpt % partSize == 0) {
								System.out.println("\t\t"+format.format(cpt)+" nodes...");
								fragmentedOutput.close();
								++part;
								csvFile = new File(basepathCsvFile + String.valueOf(part) + ".csv");
								fragmentedOutput = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(csvFile), StandardCharsets.UTF_8));
								fragmentedOutput.write("id|name|type|weight");
								fragmentedOutput.newLine();
							}						
						}else if(!line.isEmpty()){
							++cptError;
							System.err.println("error parsing node#"+format.format(cptError)+": "+line);
						}
					}
					System.out.println("\t\t"+format.format(cpt)+" nodes...");
					fragmentedOutput.close();					


					//relations
					basepathCsvFile = outputDirpath + File.separator + "relations_";
					part = 1;
					cpt = 0;
					cptError = 0;					
					csvFile = new File(basepathCsvFile + String.valueOf(part) + ".csv");
					fragmentedOutput = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(csvFile), StandardCharsets.UTF_8));
					fragmentedOutput.write("id|source|destination|type|weight");
					fragmentedOutput.newLine();
					//					csvFile = new File(outputDirpath + File.separator + "relations.csv");
					System.out.println("\t**Reading relations and converting into \""+csvFile.getName()+"\"**");		
					while(((line = in.readLine())!=null)) {
						matcher = relationPattern.matcher(line);
						if(matcher.matches()) {
							idSource = Integer.parseInt(matcher.group(2));
							if(nodeIDs.contains(idSource)) {
								idDestination = Integer.parseInt(matcher.group(3));
								if(nodeIDs.contains(idDestination)) {
									fragmentedOutput.write(matcher.group(1)+"|"+
											matcher.group(2)+"|"+
											matcher.group(3)+"|"+
											matcher.group(4)+"|"+
											matcher.group(5));
									fragmentedOutput.newLine();
									++cpt;
									if(cpt % partSize == 0) {							
										fragmentedOutput.close();
										++part;
										csvFile = new File(basepathCsvFile + String.valueOf(part) + ".csv");
										fragmentedOutput = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(csvFile), StandardCharsets.UTF_8));
										fragmentedOutput.write("id|source|destination|type|weight");
										fragmentedOutput.newLine();
										System.out.println("\t\t"+format.format(cpt)+" relations...");
									}
								}
							}
						}else if(!line.trim().isEmpty() && !line.trim().startsWith("//")){
							++cptError;
							System.err.println("error parsing edge#"+format.format(cptError)+": "+line);
						}
					}
					System.out.println("\t\t"+format.format(cpt)+" relations...");
					fragmentedOutput.close();
					res = true;					

				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		} catch (MalformedURLException e) {						
			e.printStackTrace();
		}
		if(cleanIntermediateFiles) {
			outputDir = new File(outputDirpath + File.separator + "JDM-LEXICALNET-FR");
			CreateAndLoad.deleteTemporary(outputDir);			
		}
		timer = System.currentTimeMillis() - timer;
		System.out.println("\t**Download done in: "+format.format((timer / 1_000))+" sec.**");
		return res;
	}


	public static void unzip(String zipFilepath, String destinationFolder)	{ 		
		byte[] buffer = new byte[1024];
		File newFile;
		File subfolder;
		int len;
		ZipEntry zipEntry;
		try(ZipInputStream zis = new ZipInputStream(new FileInputStream(zipFilepath))) {
			zipEntry = zis.getNextEntry();
			subfolder = new File(destinationFolder + File.separator + "JDM-LEXICALNET-FR");
			if(!subfolder.exists()) {
				subfolder.mkdir();
			}
			while (zipEntry != null) {				
				newFile = new File(destinationFolder + File.separator + zipEntry);
				try(FileOutputStream fos = new FileOutputStream(newFile)){
					while ((len = zis.read(buffer)) >= 0) {
						fos.write(buffer, 0, len);
					}
				}
				zipEntry = zis.getNextEntry();
			}
			zis.closeEntry();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch(ZipException e) {
			System.err.println("Kown exception: \""+e.getMessage()+"\". It should not interfere with the rest of the program.");
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
	}

}
