package com.aerospike.exporter;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.logging.log4j.Logger;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.policy.ScanPolicy;

import gnu.crypto.util.Base64;

public class CsvSetExporter implements Runnable {
    private final IAerospikeClient client;
    private final String namespace;
    private final String setName;
    private final Expression filterExp;
    private final File directory;
    private final Logger log;
    private boolean recordMetaData = false;
    private boolean includeDigest = false;
    private long recordLimit = Long.MAX_VALUE;

    public CsvSetExporter(IAerospikeClient client, String namespace, String setName, Expression filterExp,
            File directory, Logger log) {
        super();
        this.client = client;
        this.namespace = namespace;
        this.setName = setName;
        this.filterExp = filterExp;
        this.directory = directory;
        this.log = log;
    }

    public CsvSetExporter recordMetaData(boolean recordMetaData) {
        this.recordMetaData = recordMetaData;
        return this;
    }

    public CsvSetExporter recordLimit(long recordLimit) {
        this.recordLimit = recordLimit;
        return this;
    }
    
    public CsvSetExporter includeDigest(boolean includeDigest) {
        this.includeDigest = includeDigest;
        return this;
    }

    private void addValue(List<Object> data, Record record, String name) {
        if (record.bins.containsKey(name)) {
            Object value = record.getValue(name);
            // TODO: Put in code to handle other data types: GeoJSON, bitmap, BLOBs, etc
            data.add(value);
        } else {
            data.add(null);
        }
    }

    private synchronized void formResultAndUpdateMap(Key key, Record record, List<String> nameOrder, CSVPrinter printer)
            throws IOException {
        if (nameOrder.isEmpty()) {
            Collections.sort(nameOrder);
            if (recordMetaData) {
                nameOrder.add(0, "_generation");
                nameOrder.add(1, "_expiry");
            }
            if (includeDigest) {
                nameOrder.add(0, "_digest");
            }
        }
        List<Object> data = new ArrayList<>();
        int extraColumnCount = 0;
        if (includeDigest) {
            data.add(Base64.encode(key.digest));
            extraColumnCount++;
        }
        if (recordMetaData) {
            data.add(record.generation);
            data.add(record.expiration);
            extraColumnCount += 2;
        }
        for (int i = extraColumnCount; i < nameOrder.size(); i++) {
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

    private void rewriteFileWithHeader(File tmpFile, String outputFileName, List<String> nameOrder) throws IOException {
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

    private void processSet() throws IOException {
        ScanPolicy scanPolicy = new ScanPolicy();
        scanPolicy.concurrentNodes = false;

        // Set up the expression for the date filter
        scanPolicy.filterExp = filterExp;

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
        } catch (AerospikeException.ScanTerminated st) {
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

    @Override
    public void run() {
        try {
            this.processSet();
        } catch (IOException ioe) {
            log.error(String.format("Error processing %s.%s: %s", namespace, setName, ioe.getMessage()), ioe);
        }
    }
}
