package ddc.dbimp;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import ddc.dbio.AvroTableContext;
import ddc.support.jdbc.SqlUtils;
import ddc.support.task.TaskException;
import ddc.support.task.TaskInfo;
import ddc.support.util.FileUtil;
import ddc.support.util.LogConsole;
import ddc.support.util.LogListener;
import ddc.support.util.Statistics;
import ddc.task.impl.SqlDataDTask;
import ddc.task.model.TablePool2Config;

public class DbImp_SqlInsertTask extends SqlDataDTask {
	private final static LogListener logger = new LogConsole(DbImp_SqlInsertTask.class);
	private static final String LOG_HEADER = "Insert table - ";
	private static final String FILENAME_READING_POSTFIX = ".reading";

	public void doRun() throws Exception {
		DbImp_ConsoleConfig conf = (DbImp_ConsoleConfig) get(DbImp_ConsoleConfig.class);
		List<TablePool2Config> pools = conf.getEnabledTablePoolList();
		for (TablePool2Config pool : pools) {
			executeTablePoll(conf, pool);
		}
	}

	@SuppressWarnings("unchecked")
	private void executeTablePoll(DbImp_ConsoleConfig conf, TablePool2Config pool) throws InterruptedException, ExecutionException {
		int parallelTasks = pool.getConcurrentConnections() > 0 ? pool.getConcurrentConnections() : 1;
		logger.info(LOG_HEADER + "parallel tasks #:[" + parallelTasks + "] tables:#[" + pool.getTables().size() + "]");
		final ExecutorService executor = Executors.newFixedThreadPool(parallelTasks);
		try {
			List<Future<TaskInfo>> resultList = new ArrayList<>();
			for (AvroTableContext tableCtx : (List<AvroTableContext>) pool.getTables()) {
				Callable<TaskInfo> c = new Callable<TaskInfo>() {
					@Override
					public TaskInfo call() {
						TaskInfo tInfo = exTask(conf, pool, tableCtx);
						logger.info(LOG_HEADER + "task terminated - " + tInfo);
						return tInfo;
					}
				};
				Future<TaskInfo> result = executor.submit(c);
				resultList.add(result);
			}
			logger.info(LOG_HEADER + "executing tasks...");
			// Loop to join the threads
			for (Future<TaskInfo> future : resultList) {
				future.get();
			}
			// Loop to get the results
			List<TaskInfo> taskList = new ArrayList<>();
			resultList.forEach(x -> {
				try {
					taskList.add(x.get());
				} catch (Exception e) {
				}
			});
			// Aggregate the results
			TaskInfo gTask = TaskInfo.aggregate("Main import task", taskList);
			gTask.setSubTasks(taskList);
			// this.set(ReportAggregateTask.PROPNAME_AGGREGATED_TASK, gTask);
			// // Global report
			if (gTask.isFailed())
				throw new TaskException(gTask.getException());
			// Loop to log the results
			logger.info(gTask);
		} finally {
			// shut down the executor service now
			executor.shutdown();
			executor.awaitTermination(1, TimeUnit.HOURS);
		}
	}

	private TaskInfo exTask(DbImp_ConsoleConfig conf, TablePool2Config pool, AvroTableContext tableCtx) {
		logger.info(LOG_HEADER + "avroFile:[" + tableCtx.getAvroPath() + "] reportFile:[" + tableCtx.getReportPath() + "]");
		TaskInfo tInfo = new TaskInfo(tableCtx.getTable());
		try {
			exImportTable(tInfo.getStats(), conf, pool, tableCtx);
			return tInfo.terminateAsSucceeded();
		} catch (Throwable e) {
			logger.error(LOG_HEADER + e.getMessage(), e);
			return tInfo.terminatedAsFailed(e);
		}
	}

	private void exImportTable(Statistics stats, DbImp_ConsoleConfig conf, TablePool2Config pool, AvroTableContext tableCtx) throws Exception {
		Path avroPath = tableCtx.getAvroPath();
		Path avroReadingPath = FileUtil.postfixFileName(avroPath, FILENAME_READING_POSTFIX);
		FileUtil.rename(avroPath, avroReadingPath.getFileName().toString());
		Connection trgConn = null;
		try {
			trgConn = createConnection(pool);
			truncateOnStartup(conf, trgConn, tableCtx);
			int batchSize = pool.getBatch() > 0 ? pool.getBatch() : DEFAULT_BATCH_SIZE;
			AvroToSqlWriter avroReader = new AvroToSqlWriter();
			avroReader.execute(stats, avroReadingPath, trgConn, tableCtx.getDbTable(), batchSize);
		} catch (Throwable e) {
			if (trgConn!=null) {
				truncateOnError(conf, trgConn, tableCtx);
				throw e;
			}
		} finally {
			this.commitAndCloseConnection(trgConn);
		}
		FileUtil.rename(avroReadingPath, avroPath.getFileName().toString());
	}

	private final static int DEFAULT_BATCH_SIZE = 1000;
	private final static int DEFAULT_CONNECTION_RETRY = 1;
	private final static int DEFAULT_CONNECTION_WAIT = 10 * 1000;

//	// handle connection
//	private Connection createConnection(TablePool2Config targetPool) throws ClassNotFoundException, SQLException {
//		Connection trgConn = null;
//		// target
//		int retry = targetPool.getConnectionRetry() > 0 ? targetPool.getConnectionRetry() : DEFAULT_CONNECTION_RETRY;
//		trgConn = targetPool.getJdbcFactory().createConnection(retry, DEFAULT_CONNECTION_WAIT);
//		trgConn.setAutoCommit(false);
//		logger.info(LOG_HEADER + "target connection :[" + targetPool.getJdbcFactory().toString() + "]");
//		return trgConn;
//	}

	
	// handle connection
	private Connection createConnection(TablePool2Config targetPool) throws ClassNotFoundException, SQLException {
		Connection trgConn = null;
		// target
//		int retry = targetPool.getConnectionRetry() > 0 ? targetPool.getConnectionRetry() : DEFAULT_CONNECTION_RETRY;
		trgConn = this.getConnection(targetPool.getJdbcFactory());
		trgConn.setAutoCommit(false);
		logger.info(LOG_HEADER + "target connection :[" + targetPool.getJdbcFactory().toString() + "]");
		return trgConn;
	}

	
	private void truncateOnError(DbImp_ConsoleConfig conf, Connection conn, AvroTableContext tableCtx) throws SQLException {
		if (conf.isTruncateTargetTableOnError()) {
			String truncateSql = "TRUNCATE TABLE " + tableCtx.getTable();
			logger.info(LOG_HEADER + "truncate sql:[" + truncateSql + "]");
			SqlUtils.execute(conn, truncateSql);
		}
	}

	private void truncateOnStartup(DbImp_ConsoleConfig conf, Connection conn, AvroTableContext tableCtx) throws SQLException {
		if (conf.isTruncateTargetTableOnStartup()) {
			// Truncate
			String truncateSql = "TRUNCATE TABLE " + tableCtx.getTable();
			logger.info(LOG_HEADER + "truncate sql:[" + truncateSql + "]");
			SqlUtils.execute(conn, truncateSql);
		}
	}

}
