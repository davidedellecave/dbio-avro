package ddc.dbexp;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;

import ddc.dbio.AvroConversionException;
import ddc.dbio.AvroTableContext;
import ddc.support.task.Task;
import ddc.support.task.TaskException;
import ddc.support.task.TaskInfo;
import ddc.support.util.FileUtil;
import ddc.support.util.LogConsole;
import ddc.support.util.LogListener;
import ddc.support.util.Statistics;
import ddc.task.model.TablePool2Config;

public class DbExp_WriteAvroTask extends Task {
	private final static LogListener logger = new LogConsole(DbExp_WriteAvroTask.class);
	private static final String LOG_HEADER = "Building avro - ";
	private static final String FILENAME_WRITING_POSTFIX = ".writing";

	public void doRun() throws Exception {
		DbExp_ConsoleConfig conf = (DbExp_ConsoleConfig) get(DbExp_ConsoleConfig.class);
		List<TablePool2Config> pools = conf.getEnabledTablePoolList();
		for (TablePool2Config pool : pools) {
			executeTablePoll(conf, pool);
		}
	}

	@SuppressWarnings("unchecked")
	private void executeTablePoll(DbExp_ConsoleConfig conf, TablePool2Config pool) throws InterruptedException, ExecutionException {
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
			// Loop to join the thread
			for (Future<TaskInfo> future : resultList) {
				future.get();
			}
			//
			List<TaskInfo> taskList = new ArrayList<>();
			resultList.forEach(x -> {
				try {
					taskList.add(x.get());
				} catch (Exception e) {
				}
			});
			// Aggregate
			TaskInfo gTask = TaskInfo.aggregate("Main export task", taskList);
			gTask.setSubTasks(taskList);
//			this.set(ReportAggregateTask.PROPNAME_AGGREGATED_TASK, gTask);
			// // Global report
			if (gTask.isFailed())
				throw new TaskException(gTask.getException());
			// Loop to log the results
			logger.info(gTask);
		} finally {
			// shut down the executor service now
			executor.shutdown();
			executor.awaitTermination(1, TimeUnit.DAYS);
		}
	}

	private TaskInfo exTask(DbExp_ConsoleConfig conf, TablePool2Config pool, AvroTableContext tableCtx) {
		logger.info(LOG_HEADER + "avroFile:[" + tableCtx.getAvroPath() + "] reportFile:[" + tableCtx.getReportPath() + "]");
		TaskInfo tInfo = new TaskInfo(tableCtx.getTable());
		try {
			exExport(tInfo.getStats(), conf, pool, tableCtx);
			return tInfo.terminateAsSucceeded();
		} catch (Throwable e) {
			logger.error(LOG_HEADER + e.getMessage(), e);
			deleteOnError(conf, tableCtx);
			return tInfo.terminatedAsFailed(e);
		}
	}

	private void exExport(Statistics stats, DbExp_ConsoleConfig conf, TablePool2Config pool, AvroTableContext tableCtx) throws JsonGenerationException, JsonMappingException, ClassNotFoundException, SQLException, IOException, AvroConversionException {
		FileOutputStream avroStream = null;		
		Path avroPath = tableCtx.getAvroPath();
		Path avroWritingPath = FileUtil.postfixFileName(avroPath, FILENAME_WRITING_POSTFIX);
		try {
			if (Files.exists(avroWritingPath)) {
				Files.delete(avroWritingPath);
			}
			//Must be delete the avroPath to avoid error when trying to rename to target file
			if (conf.isOverwriteTargetFile() && Files.exists(avroPath)) {
				Files.delete(avroPath);
			}
			if (!conf.isOverwriteTargetFile() && Files.exists(avroPath)) {
				logger.warn(LOG_HEADER + "file already exists");
				return;
			}			
			avroStream = new FileOutputStream(avroWritingPath.toFile());
			SqlToAvro export = new SqlToAvro();
			export.execute(stats, pool, tableCtx, avroStream, conf.getAvroCodeFactory());
		} finally {
			IOUtils.closeQuietly(avroStream);
		}
		FileUtil.rename(avroWritingPath, avroPath.getFileName().toString());
	}
	

	private void deleteOnError(DbExp_ConsoleConfig conf, AvroTableContext tableCtx) {
		if (conf.isDeleteTargetFileOnError()) {
			doDeleteOnError(tableCtx.getAvroPath());
			doDeleteOnError(FileUtil.postfixFileName(tableCtx.getAvroPath(), FILENAME_WRITING_POSTFIX));
//			doDeleteOnError(tableCtx.getSqlPath());
//			doDeleteOnError(tableCtx.getAvroSchemaPath());
		}
	}

	private void doDeleteOnError(Path path) {
		try {
			if (Files.exists(path)) {
				logger.warn("Deleting files because an error is occurred - file:[" + path + "]");
				Files.delete(path);
			}
		} catch (Throwable e) {
			logger.error(e);
		}
	}
}
