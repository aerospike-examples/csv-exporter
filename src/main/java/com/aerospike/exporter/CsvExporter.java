package com.aerospike.exporter;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.policy.ClientPolicy;

public class CsvExporter {

    static private long startTimeInNs = 0;
    static private long endTimeInNs = Long.MAX_VALUE;
    static private long minSize = 0;
    static private long maxSize = Long.MAX_VALUE;
    static private long recordLimit = 0;
    static private boolean recordMetaData = false;
    static private boolean includeDigest = false;
    static private File directory;

    private static Logger log = LogManager.getLogger(CsvExporter.class);

    public static String[] ACCEPTABLE_DATE_FORMATS = new String[] { "MM/dd/yyyy-hh:mm:ss", "MMMM d yyyy hh:mm:ss Z" };

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("h", "host", true, "Server hostname (default: 127.0.0.1)");
        options.addOption("p", "port", true, "Server port (default: 3000)");
        options.addOption("n", "namespace", true,
                "Comma separated list of Namespaces to print, or omit for all sets. Regular expressions are supported. (default: none, print all namespace)");
        options.addOption("s", "set", true,
                "Comma separated list of Sets to print, or omit for all sets. Regular expressions are supported. (default: none, print all sets)");
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
        options.addOption("c", "concurrency", true, "Number of sets to process concurrently (default: 1)");
        options.addOption("D", "digest", true, "Include the digest in the output. (default false)");

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
        int concurrentSets = Integer.parseInt(cl.getOptionValue("concurrency", "1"));
        includeDigest = Boolean.parseBoolean(cl.getOptionValue("digest", "false"));

        if (concurrentSets <= 0) {
            log.error("Concurrent sets must be > 0, not " + concurrentSets);
            System.exit(-1);
        }
        if (cl.hasOption("usage")) {
            logUsage(options);
        } else {
            log.debug("Host: " + host);
            log.debug("Port: " + port);
            log.debug("Namespace: " + namespace);
            log.debug("Sets: " + sets);
            log.debug("Limit: " + recordLimit);
            log.debug("Date range: " + fromDate + " to " + toDate);
            log.debug("Concurrent sets: " + concurrentSets);

            startTimeInNs = fromDate == null ? 0
                    : TimeUnit.NANOSECONDS.convert(fromDate.getTime(), TimeUnit.MILLISECONDS);
            endTimeInNs = toDate == null ? Long.MAX_VALUE
                    : TimeUnit.NANOSECONDS.convert(toDate.getTime(), TimeUnit.MILLISECONDS);

            ClientPolicy cp = new ClientPolicy();
            cp.user = user;
            cp.password = password;

            IAerospikeClient client = new AerospikeClient(cp, host, port);
            dumpData(client, namespace, sets, concurrentSets);
            client.close();
        }
    }

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

    public static Date parseOptionDate(String option) throws ParseException {
        if (option != null) {
            ParseException lastException = null;
            for (String format : ACCEPTABLE_DATE_FORMATS) {
                try {
                    SimpleDateFormat sdf = new SimpleDateFormat(format);
                    return sdf.parse(option);
                } catch (ParseException pe) {
                    lastException = pe;
                    continue;
                }
            }
            // Date did not meet any of the associated formats.
            log.error("Could not parse date '" + option + "' as it didn't meet any of the expected formats: "
                    + ACCEPTABLE_DATE_FORMATS);
            throw lastException;
        } else {
            return null;
        }
    }

    private static boolean contains(Set<String> container, String name) {
        if (container.isEmpty()) {
            return true;
        }
        for (String regexSpec : container) {
            if (name.matches(regexSpec)) {
                return true;
            }
        }
        return false;
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
            if (contains(setNames, thisSetName) && contains(nsNames, thisNamespaceName)) {
                setList.add(thisNamespaceName + "." + thisSetName);
                log.debug("Added namespace {}, set {}", thisNamespaceName, thisSetName);
            }
        }
        return setList;
    }

    private static String getNamespaceName(String namesapceAndSetName) {
        int index = namesapceAndSetName.indexOf('.');
        return index > 0 ? namesapceAndSetName.substring(0, index) : namesapceAndSetName;
    }
    
    private static Set<String> getNamespacesUsingStorageEngineMemory(IAerospikeClient client, Set<String> namespaceAndSetNames) {
        Set<String> results = new HashSet<>();
        Set<String> namespacesChecked = new HashSet<>();
        for (String namespaceAndSet : namespaceAndSetNames) {
            String name = getNamespaceName(namespaceAndSet);
            if (!namespacesChecked.contains(name)) {
                String data = Info.request(client.getNodes()[0], "namespace/"+name);
                if (data.indexOf("storage-engine.data-in-memory=true") >= 0) {
                    results.add(name);
                }
                namespacesChecked.add(name);
            }
        }
        return results;
    }
    
    private static void dumpData(IAerospikeClient client, String namespaces, String sets,
            int concurrentSets) throws IOException, InterruptedException {
        ExecutorService executors = Executors.newFixedThreadPool(concurrentSets);
        Set<String> namespaceAndSetNames = getSetNames(client, namespaces, sets);
        Set<String> namespacesInMemory = getNamespacesUsingStorageEngineMemory(client, namespaceAndSetNames);
        
        // ${startTimeInNs} <= record.lastUpdateTime && ${endTimeInNs} >=
        // record.lastUpdateTime && ${minSize} <= record.deviceSize && record.deviceSize
        // <= ${maxSize}
        Expression deviceFilterExp = Exp.build(
                Exp.and(
                    Exp.le(
                        Exp.val(startTimeInNs), 
                        Exp.lastUpdate()
                    ),
                    Exp.ge(
                        Exp.val(endTimeInNs),
                        Exp.lastUpdate()
                    ), 
                    Exp.le(
                        Exp.val(minSize),
                        Exp.deviceSize()
                    ),
                    Exp.le(
                        Exp.deviceSize(),
                        Exp.val(maxSize)
                    )
                ));
        Expression memoryFilterExp = Exp.build(
                Exp.and(
                    Exp.le(
                        Exp.val(startTimeInNs), 
                        Exp.lastUpdate()
                    ),
                    Exp.ge(
                        Exp.val(endTimeInNs),
                        Exp.lastUpdate()
                    ), 
                    Exp.le(
                        Exp.val(minSize),
                        Exp.memorySize()
                    ),
                    Exp.le(
                        Exp.memorySize(),
                        Exp.val(maxSize)
                    )
                ));

        for (String namespaceAndSet : namespaceAndSetNames) {
            String[] nameParts = namespaceAndSet.split("\\.");

            String namespace = nameParts[0];
            String setName = nameParts[1];

            Expression filterExp = namespacesInMemory.contains(namespace) ? memoryFilterExp : deviceFilterExp;
            CsvSetExporter exporter = new CsvSetExporter(client, namespace, setName, filterExp, directory, log)
                    .recordLimit(recordLimit)
                    .recordMetaData(recordMetaData)
                    .includeDigest(includeDigest);
            executors.execute(exporter);
        }
        executors.shutdown();
        executors.awaitTermination(7, TimeUnit.DAYS);
        System.out.println();
    }

}
