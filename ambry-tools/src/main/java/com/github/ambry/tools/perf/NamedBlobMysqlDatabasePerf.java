/**
 * Copyright 2023 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.tools.perf;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.Account;
import com.github.ambry.account.AccountBuilder;
import com.github.ambry.account.AccountService;
import com.github.ambry.account.Container;
import com.github.ambry.account.ContainerBuilder;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.MySqlNamedBlobDbConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.frontend.Page;
import com.github.ambry.mysql.MySqlUtils;
import com.github.ambry.named.MySqlNamedBlobDbFactory;
import com.github.ambry.named.NamedBlobDb;
import com.github.ambry.named.NamedBlobRecord;
import com.github.ambry.named.PutResult;
import com.github.ambry.protocol.NamedBlobState;
import com.github.ambry.tools.util.ToolUtils;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.FileInputStream;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import javax.sql.DataSource;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.json.JSONArray;


/**
 * Tool to do performance testing on mysql database with named blob.
 *
 * This tool would insert number of target rows specified in the argument or property file and then do performance testing
 * with several named blob operations.
 *
 * To run this tool.
 * First compile ambry.jar with ./gradelw allJar
 * Then copy target/ambry.jar to a host that has access to mysql database if mysql database has host based permission checking.
 * Then run
 *  > java -cp "*" com.github.ambry.tools.perf.NamedBlobMysqlDatabasePerf
 *  >      --db_username database_username
 *  >      --db_datacenter datacenter_name
 *  >      --db_name database_name
 *  >      --db_host database_host
 *  >      --parallelism 10 // number of connections to create
 *  >      --target_row 10  // number of millions of rows to create before performance test
 *  >      --num_operations 1000000 // number of operations to do in the perforamnce test
 *  >      --enable_hard_delete true // true of false to enable hard delete. A soft delete doesn't delete rows from table
 *  >      --test_type CUSTOM // CUSTOM, LIST, READ_WRITE
 *  >      --custom.only_writes true // true of false to only do the writes for custom perf test
 *
 *  Or you can provide a property file to include all the arguments in the above command, for example:
 *  > cat named_blob.props
 *  db_username=database_username
 *  db_password=databse_pasword // yes, you can also provide database password in the prop file
 *  db_datacenter=datacenter_name
 *  db_name=database_name
 *  db_host=database_hostname
 *  parallelism=10
 *  target_rows=10
 *  num_operations=1000000
 *  perf_test=list_test
 *  > java -cp "*" com.github.ambry.tools.perf.NamedBlobMysqlDatabasePerf --props named_blob.props
 */
public class NamedBlobMysqlDatabasePerf {
  public static final String ACCOUNT_NAME_FORMAT = "ACCOUNT_%d";
  public static final String CONTAINER_NAME_FORMAT = "CONTAINER_%d";

  public static final short HUGE_LIST_ACCOUNT_ID = 1024;
  public static final short HUGE_LIST_CONTAINER_ID = 8;
  public static final String HUGE_LIST_ACCOUNT_NAME = String.format(ACCOUNT_NAME_FORMAT, HUGE_LIST_ACCOUNT_ID);
  public static final String HUGE_LIST_CONTAINER_NAME = String.format(CONTAINER_NAME_FORMAT, HUGE_LIST_CONTAINER_ID);
  public static final String HUGE_LIST_COMMON_PREFIX = "hugeListCommonPrefix/NamedBlobMysqlDatabasePerf-";
  public static final String HUGE_LIST_TEMPLATE = "hugeListCommonPrefix/NamedBlobMysqlDatabasePerf-%s/%s";

  public static final int NUMBER_ACCOUNT = 100;
  public static final int NUMBER_CONTAINER = 10;

  public static final String TABLE_NAME = "named_blobs_v2";

  public static final PartitionId PARTITION_ID = new MockPartitionId();

  public static final String DB_USERNAME = "db_username";
  public static final String DB_DATACENTER = "db_datacenter";
  public static final String DB_NAME = "db_name";
  public static final String DB_HOST = "db_host";
  public static final String DB_PASSWORD = "db_password";
  public static final String PARALLELISM = "parallelism";
  public static final String TARGET_ROWS = "target_rows";
  public static final String ENABLE_HARD_DELETE = "enable_hard_delete";
  public static final String NUM_OPERATIONS = "num_operations";
  public static final String TEST_TYPE = "test_type";
  public static final String ONLY_WRITES = "custom.only_writes";

  public enum TestType {
    READ_WRITE {
      @Override
      public Class<? extends PerformanceTestWorker> getWorkerClass() {
        return ReadWritePerformanceTestWorker.class;
      }
    }, LIST {
      @Override
      public Class<? extends PerformanceTestWorker> getWorkerClass() {
        return ListPerformanceTestWorker.class;
      }
    }, CUSTOM {
      @Override
      public Class<? extends PerformanceTestWorker> getWorkerClass() {
        return CustomPerformanceTestWorker.class;
      }
    };

    public long getExistingRows(DataSource dataSource) throws Exception {
      Class<?> clazz = getWorkerClass();
      Method method = clazz.getDeclaredMethod("getNumberOfExistingRows", DataSource.class);
      Object object = method.invoke(null, dataSource);
      if (object instanceof Long) {
        return (Long) object;
      } else {
        throw new IllegalArgumentException("getNumberOfExistingRows should return a long");
      }
    }

    public NamedBlobRecord generateNewNamedBlobRecord(Random random, List<Account> allAccounts) throws Exception {
      Class<?> clazz = getWorkerClass();
      Method method = clazz.getDeclaredMethod("generateNewNamedBlobRecord", Random.class, List.class);
      Object object = method.invoke(null, random, allAccounts);
      if (object instanceof NamedBlobRecord) {
        return (NamedBlobRecord) object;
      } else {
        throw new IllegalArgumentException("generateNewNamedBlobRecord should return a NamedBlobRecord");
      }
    }

    public abstract <T extends PerformanceTestWorker> Class<T> getWorkerClass();
  }

  public static void main(String[] args) throws Exception {
    OptionParser parser = new OptionParser();

    ArgumentAcceptingOptionSpec<String> propsFilepathOpt = parser.accepts("props", "Path to property file \n"
            + "All the argument can be provided through a property file with key=value at each line.\n"
            + "If an parameter both exist in the property file and the command line argument, the value in the command line argument would override the value in the property file")
        .withRequiredArg()
        .describedAs("props_file")
        .ofType(String.class);

    // database connection configuration
    ArgumentAcceptingOptionSpec<String> dbUsernameOpt = parser.accepts(DB_USERNAME, "Username to database")
        .withRequiredArg()
        .describedAs("db_username")
        .ofType(String.class);

    ArgumentAcceptingOptionSpec<String> dbDatacenterOpt = parser.accepts(DB_DATACENTER, "Datacenter of the database")
        .withRequiredArg()
        .describedAs("datacenter")
        .ofType(String.class);

    ArgumentAcceptingOptionSpec<String> dbNameOpt =
        parser.accepts(DB_NAME, "Database name").withRequiredArg().describedAs("db_name").ofType(String.class);

    ArgumentAcceptingOptionSpec<String> dbHostOpt =
        parser.accepts(DB_HOST, "Database host").withRequiredArg().describedAs("db_host").ofType(String.class);

    ArgumentAcceptingOptionSpec<Integer> parallelismOpt =
        parser.accepts(PARALLELISM, "Number of thread to execute sql statement")
            .withRequiredArg()
            .describedAs("parallelism")
            .ofType(Integer.class);

    ArgumentAcceptingOptionSpec<TestType> testTypeOpt =
        parser.accepts(TEST_TYPE, "Perf test type").withRequiredArg().describedAs("test_type").ofType(TestType.class);

    ArgumentAcceptingOptionSpec<Integer> numOperationsOpt =
        parser.accepts(NUM_OPERATIONS, "Number of operations to run in the performance test")
            .withRequiredArg()
            .describedAs("num_operations")
            .ofType(Integer.class);

    ArgumentAcceptingOptionSpec<Integer> targetMRowsOpt = parser.accepts(TARGET_ROWS,
            "Number of rows to insert so the total database rows would reach this target."
                + "Notice that this target is in in millions. If the value is 1, this command would make sure database would have 1 million rows.")
        .withRequiredArg()
        .describedAs("target_rows")
        .ofType(Integer.class);

    OptionSpec<Void> enabledHarDeleteOpt =
        parser.accepts(ENABLE_HARD_DELETE, "Enable hard delete to actually delete rows from table.");
    OptionSpec<Void> onlyWritesOpt =
        parser.accepts(ONLY_WRITES, "Only do the write (insert, update, delete) in the perf test.");

    OptionSet options = parser.parse(args);
    Properties props = new Properties();
    if (options.has(propsFilepathOpt)) {
      String propFilepath = options.valueOf(propsFilepathOpt);
      System.out.println("Loading properties from file " + propFilepath);
      try (FileInputStream fis = new FileInputStream(propFilepath)) {
        props.load(fis);
      }
      System.out.println("Getting properties: " + props.stringPropertyNames());
    }

    if (options.has(dbUsernameOpt)) {
      props.setProperty(DB_USERNAME, options.valueOf(dbUsernameOpt));
    }
    if (options.has(dbDatacenterOpt)) {
      props.setProperty(DB_DATACENTER, options.valueOf(dbDatacenterOpt));
    }
    if (options.has(dbNameOpt)) {
      props.setProperty(DB_NAME, options.valueOf(dbNameOpt));
    }
    if (options.has(dbHostOpt)) {
      props.setProperty(DB_HOST, options.valueOf(dbHostOpt));
    }
    if (options.has(parallelismOpt)) {
      props.setProperty(PARALLELISM, String.valueOf(options.valueOf(parallelismOpt)));
    }
    if (options.has(testTypeOpt)) {
      props.put(TEST_TYPE, options.valueOf(testTypeOpt));
    }
    if (options.has(testTypeOpt)) {
      props.put(TEST_TYPE, options.valueOf(testTypeOpt));
    }
    if (options.has(numOperationsOpt)) {
      props.setProperty(NUM_OPERATIONS, String.valueOf(options.valueOf(numOperationsOpt)));
    }
    if (options.has(enabledHarDeleteOpt)) {
      props.setProperty(ENABLE_HARD_DELETE, "true");
    }
    if (!props.containsKey(ENABLE_HARD_DELETE)) {
      props.setProperty(ENABLE_HARD_DELETE, "false");
    }
    if (options.has(onlyWritesOpt)) {
      props.setProperty(ONLY_WRITES, "true");
    }
    if (!props.containsKey(ONLY_WRITES)) {
      props.setProperty(ONLY_WRITES, "false");
    }

    List<String> requiredArguments =
        Arrays.asList(DB_USERNAME, DB_DATACENTER, DB_NAME, DB_HOST, PARALLELISM, TARGET_ROWS, TEST_TYPE, NUM_OPERATIONS,
            ENABLE_HARD_DELETE);
    for (String requiredArgument : requiredArguments) {
      if (!props.containsKey(requiredArgument)) {
        System.err.println(
            "Missing " + requiredArgument + "! Please provide it through property file or the command line argument");
        parser.printHelpOn(System.err);
        System.exit(1);
      }
    }

    // Now ask for password if it's not provided in the propFile
    if (!props.containsKey(DB_PASSWORD)) {
      String password =
          ToolUtils.passwordInput("Please input database password for user " + props.getProperty(DB_USERNAME) + ": ");
      props.setProperty(DB_PASSWORD, password);
    }

    // Now create a mysql named blob data accessor
    Properties newProperties = new Properties();
    String dbUrl =
        "jdbc:mysql://" + props.getProperty(DB_HOST) + "/" + props.getProperty(DB_NAME) + "?serverTimezone=UTC";
    MySqlUtils.DbEndpoint dbEndpoint =
        new MySqlUtils.DbEndpoint(dbUrl, props.getProperty(DB_DATACENTER), true, props.getProperty(DB_USERNAME),
            props.getProperty(DB_PASSWORD));
    JSONArray jsonArray = new JSONArray();
    jsonArray.put(dbEndpoint.toJson());
    System.out.println("DB_INFO: " + jsonArray);
    newProperties.setProperty(MySqlNamedBlobDbConfig.DB_INFO, jsonArray.toString());
    newProperties.setProperty(MySqlNamedBlobDbConfig.LIST_NAMED_BLOBS_SQL_OPTION,
        String.valueOf(MySqlNamedBlobDbConfig.MAX_LIST_NAMED_BLOBS_SQL_OPTION));
    newProperties.setProperty(MySqlNamedBlobDbConfig.LOCAL_POOL_SIZE,
        String.valueOf(2 * Integer.valueOf(props.getProperty(PARALLELISM))));
    newProperties.setProperty(MySqlNamedBlobDbConfig.ENABLE_HARD_DELETE, props.getProperty(ENABLE_HARD_DELETE));
    newProperties.setProperty(ClusterMapConfig.CLUSTERMAP_DATACENTER_NAME, props.getProperty(DB_DATACENTER));

    int numThreads = Integer.valueOf(props.getProperty(PARALLELISM));
    ScheduledExecutorService executor = Utils.newScheduler(numThreads + 1, "workers-", false);
    MetricRegistry registry = new MetricRegistry();
    // Mock an account service
    AccountService accountService = createInMemoryAccountService();
    MySqlNamedBlobDbFactory factory =
        new MySqlNamedBlobDbFactory(new VerifiableProperties(newProperties), registry, accountService);
    DataSource dataSource = factory.buildDataSource(dbEndpoint);
    NamedBlobDb namedBlobDb = factory.getNamedBlobDb();
    TestType testType = (TestType) props.get(TEST_TYPE);

    prepareDatabaseForPerfTest(testType, registry, namedBlobDb, dataSource, executor, accountService, props);
    runPerformanceTest(registry, namedBlobDb, testType, executor, accountService, numThreads, props);

    Utils.shutDownExecutorService(executor, 10, TimeUnit.SECONDS);
    namedBlobDb.close();
  }

  /**
   * Create an in memory account service for named blob.
   * @return An {@link AccountService} object.
   * @throws Exception
   */
  private static AccountService createInMemoryAccountService() throws Exception {
    AccountService accountService = new InMemAccountService(false, false);
    List<Account> accounts = new ArrayList<>();
    ThreadLocalRandom random = ThreadLocalRandom.current();
    for (int i = 0; i < NUMBER_ACCOUNT - 1; i++) {
      short accountId = (short) (i + 100);
      String accountName = String.format(ACCOUNT_NAME_FORMAT, accountId);
      int numContainer = random.nextInt(NUMBER_CONTAINER - 2) + 2;
      List<Container> containers = new ArrayList<>();
      for (int j = 0; j < numContainer; j++) {
        short containerId = (short) (j + 2);
        String containerName = String.format(CONTAINER_NAME_FORMAT, containerId);
        containers.add(
            new ContainerBuilder(containerId, containerName, Container.ContainerStatus.ACTIVE, "", accountId).build());
      }
      Account account =
          new AccountBuilder(accountId, accountName, Account.AccountStatus.ACTIVE).containers(containers).build();
      accounts.add(account);
    }
    // Now add the special account
    //@formatter:off
    accounts.add(
        new AccountBuilder(HUGE_LIST_ACCOUNT_ID, HUGE_LIST_ACCOUNT_NAME, Account.AccountStatus.ACTIVE)
            .containers(
                Collections.singletonList(
                    new ContainerBuilder(HUGE_LIST_CONTAINER_ID, HUGE_LIST_CONTAINER_NAME, Container.ContainerStatus.ACTIVE, "", HUGE_LIST_ACCOUNT_ID)
                        .build()))
            .build());
    //@formatter:on
    accountService.updateAccounts(accounts);
    return accountService;
  }

  /**
   * Fill database(table) with rows so we can reach the target number of rows.
   * @param registry The {@link MetricRegistry} object.
   * @param namedBlobDb The {@link NamedBlobDb} object.
   * @param executor The {@link ScheduledExecutorService} object.
   * @param accountService The {@link AccountService} object.
   * @param props The {@link Properties} object.
   * @throws Exception
   */
  private static void prepareDatabaseForPerfTest(TestType testType, MetricRegistry registry, NamedBlobDb namedBlobDb,
      DataSource dataSource, ScheduledExecutorService executor, AccountService accountService, Properties props)
      throws Exception {
    // First, fill the database with target number of rows
    long targetRows = Long.valueOf(props.getProperty(TARGET_ROWS)) * 1000000L;
    long existingRows = testType.getExistingRows(dataSource);
    if (existingRows >= targetRows) {
      System.out.println("Existing number of rows: " + existingRows + ", more than target number of rows: " + targetRows
          + ", skip filling database rows");
      return;
    }

    int numThreads = Integer.valueOf(props.getProperty(PARALLELISM));
    long remainingRows = targetRows - existingRows;
    System.out.println("Existing number of rows: " + existingRows + ". Target number of rows: " + targetRows
        + ". Number of rows to insert: " + remainingRows);
    AtomicLong trackingRow = new AtomicLong();
    long numberOfInsertPerWorker = remainingRows / numThreads;
    List<Future<?>> futures = new ArrayList<>();
    for (int i = 0; i < numThreads; i++) {
      long num = numberOfInsertPerWorker;
      if (i == numThreads - 1) {
        num = remainingRows - i * numberOfInsertPerWorker;
      }
      futures.add(executor.submit(new RowFillWorker(i, namedBlobDb, testType, accountService, num, trackingRow)));
    }
    AtomicBoolean stop = new AtomicBoolean(false);
    executor.submit(() -> {
      try {
        while (!stop.get()) {
          System.out.println(trackingRow.get() + " rows has been inserted");
          Thread.sleep(1000);
        }
      } catch (Exception e) {
      }
    });
    for (Future<?> future : futures) {
      future.get();
    }
    System.out.println("All the RowFillWorkers are finished");
    stop.set(true);
    printHistogramMetric(registry, "com.github.ambry.named.MySqlNamedBlobDb.NamedBlobPutTimeInMs");
  }

  /**
   * Print out histogram data for the given metric name.
   * @param registry The {@link MetricRegistry} object.
   * @param metricName The metric name to print out
   */
  private static void printHistogramMetric(MetricRegistry registry, String metricName) {
    Histogram histogram = registry.getHistograms().get(metricName);
    System.out.println("Metric of " + metricName + ": ");
    System.out.println("Count: " + histogram.getCount());
    System.out.println("Median: " + histogram.getSnapshot().getMedian());
    System.out.println("95th: " + histogram.getSnapshot().get95thPercentile());
    System.out.println("99th: " + histogram.getSnapshot().get99thPercentile());
  }

  /**
   * Worker class to insert rows into database.
   */
  public static class RowFillWorker implements Runnable {
    private final int id;
    private final NamedBlobDb namedBlobDb;
    private final List<Account> allAccounts;
    private final long numberOfInsert;
    private final TestType testType;
    private final AtomicLong trackingRow;

    /**
     * Constructor for this class.
     * @param id The id of this worker.
     * @param namedBlobDb The {@link NamedBlobDb} object.
     * @param accountService The {@link AccountService} object.
     * @param numberOfInsert The insert of rows to insert
     * @param trackingRow The {@link AtomicLong} object to keep track of how many rows are inserted.
     */
    public RowFillWorker(int id, NamedBlobDb namedBlobDb, TestType testType, AccountService accountService,
        long numberOfInsert, AtomicLong trackingRow) {
      this.id = id;
      this.namedBlobDb = namedBlobDb;
      this.testType = testType;
      this.numberOfInsert = numberOfInsert;
      this.trackingRow = trackingRow;
      allAccounts = new ArrayList<>(accountService.getAllAccounts());
    }

    @Override
    public void run() {
      ThreadLocalRandom random = ThreadLocalRandom.current();
      try {
        for (long l = 0; l < numberOfInsert; l++) {
          NamedBlobRecord record = testType.generateNewNamedBlobRecord(random, allAccounts);
          namedBlobDb.put(record).get();
          trackingRow.incrementAndGet();
        }
        System.out.println("RowFillWorker " + id + " finishes writing " + numberOfInsert + " records");
      } catch (Exception e) {
        System.out.println("RowFillWorker " + id + " has som exception " + e);
      }
    }
  }

  /**
   * Run performance test after target number of rows are filled in the database.
   * @param registry The {@link MetricRegistry} object.
   * @param namedBlobDb The {@link NamedBlobDb} object.
   * @param executor The {@link ScheduledExecutorService} object.
   * @param accountService The {@link AccountService} object.
   * @param numThreads The number of threads to run performance test
   * @throws Exception
   */
  private static void runPerformanceTest(MetricRegistry registry, NamedBlobDb namedBlobDb, TestType testType,
      ScheduledExecutorService executor, AccountService accountService, int numThreads, Properties props)
      throws Exception {
    long numberOfPuts = 1000 * 1000; // 1 million inserts
    System.out.println("Running performance test, number of puts: " + numberOfPuts);
    long numberOfInsertPerWorker = numberOfPuts / numThreads;
    List<Future<?>> futures = new ArrayList<>();
    for (int i = 0; i < numThreads; i++) {
      long num = numberOfInsertPerWorker;
      if (i == numThreads - 1) {
        num = numberOfPuts - i * numberOfInsertPerWorker;
      }
      futures.add(executor.submit(
          (PerformanceTestWorker) testType.getWorkerClass().getConstructors()[0].newInstance(i, namedBlobDb,
              accountService, num, props)));
    }
    for (Future<?> future : futures) {
      future.get();
    }
    System.out.println("All the PerformanceTestWorkers are finished");
    printHistogramMetric(registry, "com.github.ambry.named.MySqlNamedBlobDb.NamedBlobPutTimeInMs");
    printHistogramMetric(registry, "com.github.ambry.named.MySqlNamedBlobDb.NamedBlobGetTimeInMs");
    printHistogramMetric(registry, "com.github.ambry.named.MySqlNamedBlobDb.NamedTtlupdateTimeInMs");
    printHistogramMetric(registry, "com.github.ambry.named.MySqlNamedBlobDb.NamedBlobListTimeInMs");
    printHistogramMetric(registry, "com.github.ambry.named.MySqlNamedBlobDb.NamedBlobDeleteTimeInMs");
  }

  public static abstract class PerformanceTestWorker implements Runnable {
    protected final int id;
    protected final NamedBlobDb namedBlobDb;
    protected final AccountService accountService;
    protected final List<Account> allAccounts;
    protected final long numberOfOperations;
    protected final Properties props;

    public PerformanceTestWorker(int id, NamedBlobDb namedBlobDb, AccountService accountService, int numberOfOperations,
        Properties props) {
      this.id = id;
      this.namedBlobDb = namedBlobDb;
      this.accountService = accountService;
      this.numberOfOperations = numberOfOperations;
      this.props = props;
      allAccounts = new ArrayList<>(accountService.getAllAccounts());
    }
  }

  public static class ReadWritePerformanceTestWorker extends PerformanceTestWorker {
    private final List<NamedBlobRecord> allRecords = new ArrayList<>();

    public ReadWritePerformanceTestWorker(int id, NamedBlobDb namedBlobDb, AccountService accountService,
        int numberOfOperations, Properties props) {
      super(id, namedBlobDb, accountService, numberOfOperations, props);
    }

    @Override
    public void run() {
      ThreadLocalRandom random = ThreadLocalRandom.current();
      try {
        for (long l = 0; l < numberOfOperations; l++) {
          NamedBlobRecord record = ReadWritePerformanceTestWorker.generateNewNamedBlobRecord(random, allAccounts);
          namedBlobDb.put(record, NamedBlobState.IN_PROGRESS, true).get();
          allRecords.add(record);
        }
        System.out.println(
            "ReadWritePerformanceTestWorker " + id + " finishes writing " + numberOfOperations + " records");

        for (NamedBlobRecord record : allRecords) {
          namedBlobDb.updateBlobTtlAndStateToReady(record).get();
        }
        System.out.println(
            "ReadWritePerformanceTestWorker " + id + " finishes updating " + numberOfOperations + " records");

        for (NamedBlobRecord record : allRecords) {
          namedBlobDb.get(record.getAccountName(), record.getContainerName(), record.getBlobName()).get();
        }
        System.out.println(
            "ReadWritePerformanceTestWorker " + id + " finishes reading " + numberOfOperations + " records");

        for (NamedBlobRecord record : allRecords) {
          namedBlobDb.delete(record.getAccountName(), record.getContainerName(), record.getBlobName()).get();
        }
        System.out.println(
            "ReadWritePerformanceTestWorker " + id + " finishes deleting " + numberOfOperations + " records");
      } catch (Exception e) {
        System.out.println("ReadWritePerformanceTestWorker " + id + " has som exception " + e);
      }
    }

    /**
     * Get total number of rows of the target table.
     * @param datasource Datasource to execute query on.
     * @return Total number of rows
     * @throws Exception
     */
    public static long getNumberOfExistingRows(DataSource datasource) throws Exception {
      String rowQuerySql = "SELECT COUNT(*) as total FROM " + TABLE_NAME;
      long numberOfRows = 0;
      try (Connection connection = datasource.getConnection()) {
        try (PreparedStatement queryStatement = connection.prepareStatement(rowQuerySql)) {
          try (ResultSet result = queryStatement.executeQuery()) {
            while (result.next()) {
              numberOfRows = result.getLong("total");
            }
          }
        }
      }
      return numberOfRows;
    }

    /**
     * Generate a random {@link NamedBlobRecord}.
     * @param random The {@link Random} object to generate random number.
     * @param allAccounts All the accounts in the account service.
     * @return A {@link NamedBlobRecord}.
     */
    public static NamedBlobRecord generateNewNamedBlobRecord(Random random, List<Account> allAccounts) {
      Account account = allAccounts.get(random.nextInt(allAccounts.size()));
      List<Container> containers = new ArrayList<>(account.getAllContainers());
      Container container = containers.get(random.nextInt(containers.size()));
      String blobName = TestUtils.getRandomString(50);
      BlobId blobId =
          new BlobId(BlobId.BLOB_ID_V6, BlobId.BlobIdType.NATIVE, (byte) 1, account.getId(), container.getId(),
              PARTITION_ID, false, BlobId.BlobDataType.DATACHUNK);
      NamedBlobRecord record =
          new NamedBlobRecord(account.getName(), container.getName(), blobName, blobId.toString(), -1);
      return record;
    }
  }

  public static class ListPerformanceTestWorker extends PerformanceTestWorker {

    public ListPerformanceTestWorker(int id, NamedBlobDb namedBlobDb, AccountService accountService,
        int numberOfOperations, Properties props) {
      super(id, namedBlobDb, accountService, numberOfOperations, props);
    }

    @Override
    public void run() {
      try {
        for (int i = 0; i < numberOfOperations; i++) {
          Page<NamedBlobRecord> page = null;
          String token = null;
          do {
            page =
                namedBlobDb.list(HUGE_LIST_ACCOUNT_NAME, HUGE_LIST_CONTAINER_NAME, HUGE_LIST_COMMON_PREFIX, null, null)
                    .get();
            token = page.getNextPageToken();
          } while (token != null);
          if (i % 100 == 0) {
            System.out.println("ListPerformanceTestWorker " + id + " finishes " + i + " operations");
          }
        }
      } catch (Exception e) {
        System.out.println("ListPerformanceTestWorker " + id + " has som exception " + e);
      }
    }

    public static long getNumberOfExistingRows(DataSource datasource) throws Exception {
      String rowQuerySql = "SELECT COUNT(*) as total FROM " + TABLE_NAME
          + " WHERE account_id = ? And container_id = ? and BLOB_NAME like ?";
      long numberOfRows = 0;
      try (Connection connection = datasource.getConnection()) {
        try (PreparedStatement queryStatement = connection.prepareStatement(rowQuerySql)) {
          queryStatement.setInt(1, HUGE_LIST_ACCOUNT_ID);
          queryStatement.setInt(2, HUGE_LIST_CONTAINER_ID);
          queryStatement.setString(3, HUGE_LIST_COMMON_PREFIX + "%");
          try (ResultSet result = queryStatement.executeQuery()) {
            while (result.next()) {
              numberOfRows = result.getLong("total");
            }
          }
        }
      }
      return numberOfRows;
    }

    /**
     * Generate a random {@link NamedBlobRecord}.
     * @param random The {@link Random} object to generate random number.
     * @param allAccounts All the accounts in the account service.
     * @return A {@link NamedBlobRecord}.
     */
    public static NamedBlobRecord generateNewNamedBlobRecord(Random random, List<Account> allAccounts) {
      String blobName = String.format(HUGE_LIST_TEMPLATE, UUID.randomUUID(), TestUtils.getRandomString(32));
      BlobId blobId = new BlobId(BlobId.BLOB_ID_V6, BlobId.BlobIdType.NATIVE, (byte) 1, HUGE_LIST_ACCOUNT_ID,
          HUGE_LIST_CONTAINER_ID, PARTITION_ID, false, BlobId.BlobDataType.DATACHUNK);
      NamedBlobRecord record =
          new NamedBlobRecord(HUGE_LIST_ACCOUNT_NAME, HUGE_LIST_CONTAINER_NAME, blobName, blobId.toString(), -1);
      return record;
    }
  }

  public static class CustomPerformanceTestWorker extends PerformanceTestWorker {
    private final boolean onlyWrites;

    public CustomPerformanceTestWorker(int id, NamedBlobDb namedBlobDb, AccountService accountService,
        int numberOfOperations, Properties props) {
      super(id, namedBlobDb, accountService, numberOfOperations, props);
      this.onlyWrites = props.getProperty(ONLY_WRITES).equalsIgnoreCase("true");
    }

    @Override
    public void run() {
      ThreadLocalRandom random = ThreadLocalRandom.current();
      try {
        for (long l = 1; l <= numberOfOperations; l++) {
          NamedBlobRecord record = CustomPerformanceTestWorker.generateNewNamedBlobRecord(random, allAccounts);
          if (!onlyWrites) {
            try {
              namedBlobDb.get(record.getAccountName(), record.getContainerName(), record.getBlobName()).get();
            } catch (Exception e) {
              // expected NOT_FOUND failure
            }
            try {
              namedBlobDb.get(record.getAccountName(), record.getContainerName(), record.getBlobName() + "/").get();
            } catch (Exception e) {
              // expected NOT_FOUND failure
            }
          }
          PutResult putResult = namedBlobDb.put(record, NamedBlobState.IN_PROGRESS, true).get();
          // Get the updated version
          record = putResult.getInsertedRecord();
          namedBlobDb.updateBlobTtlAndStateToReady(record).get();
          if (!onlyWrites) {
            // Get blob again
            namedBlobDb.get(record.getAccountName(), record.getContainerName(), record.getBlobName()).get();
          }
          // Now delete
          namedBlobDb.delete(record.getAccountName(), record.getContainerName(), record.getBlobName()).get();
          if (l % 100 == 0) {
            System.out.println("CustomPerformanceTestWorker " + id + " finishes " + l + " records");
          }
        }
        System.out.println("CustomPerformanceTestWorker " + id + " finishes " + numberOfOperations + " records");
      } catch (Exception e) {
        System.out.println("CustomPerformanceTestWorker " + id + " has som exception " + e);
      }
    }

    public static long getNumberOfExistingRows(DataSource datasource) throws Exception {
      return 0L;
    }

    public static NamedBlobRecord generateNewNamedBlobRecord(Random random, List<Account> allAccounts) {
      Account account = allAccounts.get(random.nextInt(allAccounts.size()));
      List<Container> containers = new ArrayList<>(account.getAllContainers());
      Container container = containers.get(random.nextInt(containers.size()));
      String blobName =
          String.format("checkpoints/%s/chk-900/%s", TestUtils.getRandomString(32), UUID.randomUUID().toString());
      BlobId blobId =
          new BlobId(BlobId.BLOB_ID_V6, BlobId.BlobIdType.NATIVE, (byte) 1, account.getId(), container.getId(),
              PARTITION_ID, false, BlobId.BlobDataType.DATACHUNK);
      long expirationTime = Utils.addSecondsToEpochTime(System.currentTimeMillis() / 1000, TimeUnit.DAYS.toSeconds(2));
      NamedBlobRecord record =
          new NamedBlobRecord(account.getName(), container.getName(), blobName, blobId.toString(), expirationTime);
      return record;
    }
  }
}
