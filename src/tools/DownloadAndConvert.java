package tools;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
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

import net.lingala.zip4j.ZipFile;
import net.lingala.zip4j.exception.ZipException;

public class DownloadAndConvert {

	public static void main(String[] args) throws IOException {
		downloadAndCSVConvert("__tmpRezoJDMCSV");
	}

	/**
	 * URL to most recent dump rezoJDM
	 */
	private static final String LAST_OUTPUT_NOHTML = "JDM-LEXICALNET-FR/LAST_OUTPUT_NOHTML.txt";
	private static final String REZO_BASE_URL =  "http://www.jeuxdemots.org/";
	private static final DecimalFormat format = new DecimalFormat();


	private static final Pattern relationTypesPattern = Pattern.compile(
			"^rtid=(\\d+)\\|"
					+ "name=\"(.*)\"\\|"
					+ "nom_etendu=\"(.*)\"\\|"
					+ "info=\"(.*)\"$");
	private static final Pattern nodeTypesPattern = Pattern.compile(
			"^ntid=(\\d+)\\|"
					+ "name=\"(.*)\"\\|"			
					+ "info=\"(.*)\"$");

	private static final Pattern nodePattern = Pattern.compile(
			"^eid=(\\d+)\\|"
					+ "n=\"(.*)\"\\|"
					+ "t=(\\d+)\\|"
					+ "w=(\\d+)$");			

	private static final Pattern relationPattern = Pattern.compile(
			"^rid=(\\d+)\\|"
					+ "n1=(\\d+)\\|"
					+ "n2=(\\d+)\\|"
					+ "t=(\\d+)\\|"
					+ "w=(\\d+)$");	


	public static boolean downloadAndCSVConvert(String outputDirpath) {	
		Matcher matcher;
		long timer = System.currentTimeMillis();			
		boolean res = false;
		File outputDir, csvFile, zipFile;
		String dumppath;
		Set<Integer> nodeIDs;
		Integer id, idSource, idDestination;
		try {
			URL url = new URL(REZO_BASE_URL + LAST_OUTPUT_NOHTML);
			String line = null;			
			int cpt;
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
				System.out.println("\t**Downloading \""+REZO_BASE_URL + line + ".zip\"**");
				dumppath = outputDirpath + File.separator + "lastdump.zip";
				zipFile = new File(dumppath);
				if(zipFile.exists()) {
					zipFile.delete();
				}
				try {
					Files.copy(url.openStream(),Paths.get(dumppath));
				} catch (IOException e) {
					e.printStackTrace();
					return false;
				}
				System.out.println("\t**Unzipping...**");
				unzip(dumppath, outputDirpath);

				try(BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(outputDirpath + File.separator + line), "cp1252"))){	
					//header
					System.out.println("\t**Skipping header...**");
					while((line = in.readLine())!=null && !line.startsWith("// ---- RELATION TYPES")) {}					
					//relation types
					csvFile = new File(outputDirpath + File.separator + "relationTypes.csv");
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
							}						
						}
					}

					//node types
					csvFile = new File(outputDirpath + File.separator + "nodeTypes.csv");
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
							}	
						}
					}	

					//nodes
					nodeIDs = new HashSet<>();
					csvFile = new File(outputDirpath + File.separator + "nodes.csv");
					System.out.println("\t**Reading nodes and converting into \""+csvFile.getName()+"\"**");
					cpt = 0;
					try (BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(csvFile), StandardCharsets.UTF_8))) {
						out.write("id|name|type|weight");
						out.newLine();
						while((line = in.readLine())!=null && !line.startsWith("// -- RELATIONS")) {
							matcher = nodePattern.matcher(line);
							if(matcher.matches()) {
								id = Integer.parseInt(matcher.group(1));
								nodeIDs.add(id);
								out.write(matcher.group(1)+"|"+
										matcher.group(2)+"|"+
										matcher.group(3)+"|"+
										matcher.group(4)+"|");
								out.newLine();
							}
							++cpt;
							if(cpt % 1_000_000 == 0) {
								System.out.println("\t\t"+format.format(cpt)+" nodes...");
							}
						}
					}

					//relations
					csvFile = new File(outputDirpath + File.separator + "relations.csv");
					System.out.println("\t**Reading relations and converting into \""+csvFile.getName()+"\"**");
					cpt = 0;
					try (BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(csvFile), StandardCharsets.UTF_8))) {
						out.write("id|source|destination|type|weight");
						out.newLine();
						while(((line = in.readLine())!=null)) {
							matcher = relationPattern.matcher(line);
							if(matcher.matches()) {
								idSource = Integer.parseInt(matcher.group(2));
								if(nodeIDs.contains(idSource)) {
									idDestination = Integer.parseInt(matcher.group(3));
									if(nodeIDs.contains(idDestination)) {
										out.write(matcher.group(1)+"|"+
												matcher.group(2)+"|"+
												matcher.group(3)+"|"+
												matcher.group(4)+"|"+
												matcher.group(5));
										out.newLine();
									}
								}
							}							
							++cpt;
							if(cpt % 1_000_000 == 0) {
								System.out.println("\t\t"+format.format(cpt)+" relations...");
							}
						}
						res = true;
					}									
				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		} catch (MalformedURLException e) {						
			e.printStackTrace();
		}
		timer = System.currentTimeMillis() - timer;
		System.out.println("\t**Download done in: "+format.format((timer / 1_000))+" sec.**");
		return res;
	}


	public static void unzip(String zipFilepath, String destinationFolder){	   
		try {
			ZipFile zipFile = new ZipFile(zipFilepath);	        
			zipFile.extractAll(destinationFolder);
		} catch (ZipException e) {
			e.printStackTrace();
		}
	}
}
