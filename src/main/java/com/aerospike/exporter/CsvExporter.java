package com.aerospike.exporter;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.query.PredExp;

public class CsvExporter {

	static private long startTimeInNs = 0;
	static private long endTimeInNs = Long.MAX_VALUE;
	static private long minSize = 0;
	static private long maxSize = Long.MAX_VALUE;
	static private long recordLimit = 0;
	static private boolean recordMetaData = false;
	static private File directory;
	
    private static Logger log = LogManager.getLogger(CsvExporter.class);

    /**
     * Write usage to console.
     */
    private static void logUsage(Options options) {
    	HelpFormatter formatter = new HelpFormatter();
    	StringWriter sw = new StringWriter();
    	PrintWriter pw = new PrintWriter(sw);
    	String syntax = CsvExporter.class.getName() + " [<options>]";
    	formatter.printHelp(pw, 100, syntax, "options:", options, 0, 2, null);
    	log.info(sw.toString());
    }

    /*
    public static void main2(String[] args) throws Exception {
    	AerospikeClient client = new AerospikeClient(null, "172.28.128.5", 3000);
    	// Create a 16kB string
    	StringBuffer sb = new StringBuffer(200000);
    	for (int i = 0; i < 1024; i++) {
    		sb.append("12567890123456");
    	}
    	
    	// Insert records in the database
    	long now = System.nanoTime();
    	for (int j = 0; j < 2; j++) {
        	for (int i = 0; i < 655360; i++) {
        		client.put(null, new Key("test", "sizeTest", i), new Bin("data", sb.toString()));
        		if (i%1024 == 1023) {
        			System.out.printf("Done %dk in %.2fs\n", ((i+1)/1024), (System.nanoTime()-now)/1_000_000_000.0);
        		}
        	}
    	}
    	System.out.println("Done");
    	client.close();
    }
    */
    public static String[] ACCEPTABLE_DATE_FORMATS = new String[] {
    		"MM/dd/yyyy-hh:mm:ss",
    		"MMMM d yyyy hh:mm:ss Z"
    };
    
    public static Date parseOptionDate(String option) throws ParseException {
    	if (option != null) {
    		ParseException lastException = null;
    		for (String format : ACCEPTABLE_DATE_FORMATS) {
    			try {
    				SimpleDateFormat sdf = new SimpleDateFormat(format);
    				return sdf.parse(option);
    			}
    			catch (ParseException pe) {
    				lastException = pe;
    				continue;
    			}
    		}
    		// Date did no meet any of the associated formats.
    		log.error("Could not parse date '" + option + "' as it didn't meet any of the expected formats: " + ACCEPTABLE_DATE_FORMATS);
    		throw lastException; 
    	}
    	else {
    		return null;
    	}
    }
    
    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("h", "host", true, "Server hostname (default: 127.0.0.1)");
        options.addOption("p", "port", true, "Server port (default: 3000)");
        options.addOption("n", "namespace", true, "Comma separated list of Namespaces to print, or omit for all sets (default: none, print all namespace)");
        options.addOption("s", "set", true, "Comma separated list of Sets to print, or omit for all sets (default: none, print all sets)");
        options.addOption("u", "usage", false, "Print usage");
        options.addOption("d", "directory", true, "Output directory (default: .)");
        options.addOption("f", "from", true, "From time (accepts MM/dd/yyyy-hh:mm:ss) (default: null)");
        options.addOption("t", "to", true, "To time (accepts MM/dd/yyyyhh:mm:ss) (default: null)");
        options.addOption("U", "user", true, "User for secured clusters (optional)");
        options.addOption("P", "password", true, "Password for secured clusters (optional)");
        options.addOption("l", "limit", true, "Limit for the number of records. 0 for unlimited. (default: 0)");
        options.addOption("m", "metadata", true, "Whether to record metadata in the records or not (default: false)");
        options.addOption("I", "minSize", true, "Minimum size of records to print (default: 0)");
        options.addOption("A", "maxSize", true, "Maximum size of records to print (default: unlimited)");

        CommandLineParser parser = new DefaultParser();
        CommandLine cl = parser.parse(options, args, false);

        String host = cl.getOptionValue("h", "127.0.0.1");
        String portString = cl.getOptionValue("p", "3000");
        int port = Integer.parseInt(portString);
        String namespace = cl.getOptionValue("n", "test");
        String sets = cl.getOptionValue("s");
        recordLimit = Long.parseLong(cl.getOptionValue("l", "0"));
        Date fromDate = parseOptionDate(cl.getOptionValue("from"));
        Date toDate = parseOptionDate(cl.getOptionValue("to"));
        String user = cl.getOptionValue("user");
        String password = cl.getOptionValue("password");
        directory = new File(cl.getOptionValue("directory", "."));
        recordMetaData = Boolean.parseBoolean(cl.getOptionValue("m", "false"));
        minSize = Long.parseLong(cl.getOptionValue("minSize", "0"));
        maxSize = Long.parseLong(cl.getOptionValue("maxSize", "" + Long.MAX_VALUE));
        
        if (cl.hasOption("usage")) {
        	logUsage(options);
        }
        else {
            log.debug("Host: " + host);
            log.debug("Port: " + port);
            log.debug("Namespace: " + namespace);
            log.debug("Sets: " + sets);
            log.debug("Limit: " + recordLimit);
            log.debug("Date range: " + fromDate + " to " + toDate);
            
            startTimeInNs = fromDate == null ? 0 : TimeUnit.NANOSECONDS.convert(fromDate.getTime(), TimeUnit.MILLISECONDS);
            endTimeInNs = toDate == null ? Long.MAX_VALUE : TimeUnit.NANOSECONDS.convert(toDate.getTime(), TimeUnit.MILLISECONDS);
            
            ClientPolicy cp = new ClientPolicy();
            cp.user = user;
            cp.password = password;
            
            IAerospikeClient client = new AerospikeClient(cp, host, port);
            dumpData(client, namespace, sets);
            client.close();
        }
	}


    private static Set<String> getSetNames(IAerospikeClient client, String namespaces, String sets) {
    	Set<String> setNames = new HashSet<>();
    	Set<String> nsNames = new HashSet<>();
    	if (namespaces != null) {
    		nsNames.addAll(Arrays.asList(namespaces.split(",")));
    	}
    	if (sets != null) {
    		setNames.addAll(Arrays.asList(sets.split(",")));
    	}

    	Set<String> setList = new HashSet<>();
		String data = Info.request(client.getNodes()[0], "sets");
		String[] setDetails = data.split(";");
		for (String thisSet : setDetails) {
			String[] thisSetDetails = thisSet.split(":");
			String thisSetName = null;
			String thisNamespaceName = null;
			for (String thisDetail : thisSetDetails) {
				String[] pair = thisDetail.split("=");
				switch (pair[0]) {
				case "ns":
					thisNamespaceName = pair[1];
					break;
				case "set":
					thisSetName = pair[1];
					break;
				}
			}
			if ((setNames.isEmpty() || setNames.contains(thisSetName)) && 
					(nsNames.isEmpty() || nsNames.contains(thisNamespaceName))) {
				setList.add(thisNamespaceName + "." + thisSetName);
				log.debug("Added namespace {}, set {}", thisNamespaceName, thisSetName);
			}
		}
    	return setList;
    }
    
    private static void addValue(List<Object> data, Record record, String name) {
    	if (record.bins.containsKey(name)) {
    		Object value = record.getValue(name);
    		data.add(value);
    	}
    	else {
    		// TODO: Should this be an empty string?
    		data.add(null);
    	}
    }
    private static synchronized void formResultAndUpdateMap(Key key, Record record, List<String> nameOrder, CSVPrinter printer) throws IOException {
    	if (nameOrder.isEmpty()) {
    		Collections.sort(nameOrder);
    		if (recordMetaData) {
    			nameOrder.add(0, "_generation");
    			nameOrder.add(1, "_expiry");
    		}
    	}
    	List<Object> data = new ArrayList<>();
		if (recordMetaData) {
			data.add(record.generation);
			data.add(record.expiration);
		}
		for (int i = recordMetaData ? 2 : 0; i < nameOrder.size(); i++) {
			addValue(data, record, nameOrder.get(i));
		}
		// We need to add any new columns too.
		for (String thisKey : record.bins.keySet()) {
			if (!nameOrder.contains(thisKey)) {
				nameOrder.add(thisKey);
				addValue(data, record, thisKey);
			}
		}
		printer.printRecord(data);
    }
    
    private static void rewriteFileWithHeader(File tmpFile, String outputFileName, List<String> nameOrder) throws IOException {
    	OutputStream writer = new BufferedOutputStream(new FileOutputStream(new File(directory, outputFileName)));
    	for (String name : nameOrder) {
    		writer.write(name.getBytes());
    		writer.write(',');
    	}
    	writer.write('\n');    	
    	Files.copy(Paths.get(tmpFile.getAbsolutePath()), writer);
    	writer.close();
    	Files.delete(Paths.get(tmpFile.getAbsolutePath()));
    }
    
    private static void dumpSingleSetData(IAerospikeClient client, String namespace, String setName) throws IOException {
		ScanPolicy scanPolicy = new ScanPolicy();
		scanPolicy.concurrentNodes = false;
		
		// Set up the pred-exp for the date filter
		scanPolicy.predExp = new PredExp[] {
				PredExp.integerValue(startTimeInNs),
				PredExp.recLastUpdate(),
				PredExp.integerLessEq(),
				PredExp.integerValue(endTimeInNs),
				PredExp.recLastUpdate(),
				PredExp.integerGreaterEq(),
				PredExp.integerValue(minSize),
				PredExp.recDeviceSize(),
				PredExp.integerLessEq(),
				PredExp.integerValue(maxSize),
				PredExp.recDeviceSize(),
				PredExp.integerGreaterEq(),
				PredExp.and(4)
		};
		
		
		String combinedFilePrefix = namespace + "." + setName;
		final File outputFile = File.createTempFile(combinedFilePrefix, ".csv", directory);
    	final CSVPrinter printer = new CSVPrinter(new FileWriter(outputFile), CSVFormat.DEFAULT);
		final Vector<String> nameOrder = new Vector<>();
		final AtomicLong counter = new AtomicLong(0);
		
		try {
			client.scanAll(scanPolicy, namespace, setName, new ScanCallback() {
				
				@Override
				public void scanCallback(Key key, Record record) throws AerospikeException {
					try {
						formResultAndUpdateMap(key, record, nameOrder, printer);
						long count = counter.incrementAndGet(); 
						if (recordLimit > 0 && count >= recordLimit) {
							throw new AerospikeException.ScanTerminated();
						}
					} catch (IOException e) {
						throw new AerospikeException.ScanTerminated(e);
					}
				}
			});
		}
		catch (AerospikeException.ScanTerminated st) {
			// this is expected unless st has an exception attached
			if (st.getCause() != null) {
				printer.close();
				throw st;
			}
		}
		log.debug("Namespace: {}, Set: {}, output {} records", namespace, setName, counter.get());
		printer.close();
		rewriteFileWithHeader(outputFile, combinedFilePrefix + ".csv", nameOrder);
    }
	private static void dumpData(IAerospikeClient client, String namespaces, String sets) throws IOException {
		
		Set<String> namespaceAndSetNames = getSetNames(client, namespaces, sets);
		for (String namespaceAndSet : namespaceAndSetNames) {
			String[] nameParts = namespaceAndSet.split("\\.");
			
			String namespace = nameParts[0];
			String setName = nameParts[1];
			
			dumpSingleSetData(client, namespace, setName);
			
		}
		System.out.println();
//		StringWriter out = new StringWriter();
//		CSVPrinter printer = new CSVPrinter(out, CSVFormat.EXCEL);
//		printer.
	}

}
