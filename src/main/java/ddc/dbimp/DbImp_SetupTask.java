package ddc.dbimp;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import ddc.dbio.AvroTableContext;
import ddc.dbio.PathProvider;
import ddc.support.task.Task;
import ddc.support.task.TaskException;
import ddc.support.util.FileUtil;
import ddc.support.util.LogConsole;
import ddc.support.util.LogListener;
import ddc.task.model.TableConfig;
import ddc.task.model.TablePool2Config;

public class DbImp_SetupTask extends Task {
	private final static LogListener logger = new LogConsole(DbImp_SetupTask.class);
	private static final String LOG_HEADER = "Setup - ";

	public void doRun() {
		DbImp_ConsoleConfig conf = (DbImp_ConsoleConfig) get(DbImp_ConsoleConfig.class);
		this.set("DBIOConfig", conf);
		
		if (!FileUtil.isReadbleFolder(conf.getSourceFolder())) {
			throw new TaskException(LOG_HEADER + "source folder is not valid :[" + conf.getSourceFolder() + "]");
		}
		
		for (TablePool2Config pool : conf.getTablePoolList()) {
			if (!pool.isEnabled()) {
				logger.warn(LOG_HEADER + "Pool is not enabled:[" + pool.toString() + "]");
			}  else {
				logger.info(LOG_HEADER + "Pool is enabled:[" + pool.toString() + "]");
				for (TableConfig tableConf : pool.getTables()) {
					if (tableConf.isEnabled()) {
						logger.warn(LOG_HEADER + "Table is enabled:[" + tableConf.getTable() + "]");		
					} else {
						logger.info(LOG_HEADER + "Table is not enabled:[" + tableConf.getTable() + "]");		
					}
 				}
			}
		}
		
		List<TablePool2Config> pools = conf.getEnabledTablePoolList();
		for (TablePool2Config pool : pools) {
			List<AvroTableContext> list = new ArrayList<>();
			for (TableConfig tableConf : pool.getTables()) {
				AvroTableContext ctx = new AvroTableContext(tableConf);
				ctx.setSignature(PathProvider.getSignature(pool, tableConf, conf.getOverrideMaxRows()));
				ctx.setAvroPath(PathProvider.getAvroSource(conf, pool, tableConf));
				if (!Files.exists(ctx.getAvroPath())) {
					throw new TaskException(LOG_HEADER + "Avro file not found - file:[" + ctx.getAvroPath() + "]");					
				}
				ctx.setReportPath(Paths.get(ctx.getAvroPath().toString() + ".report"));
				ctx.setSourceSqlSchemaPath(PathProvider.getSource(conf, pool, tableConf, "source.sql"));
				ctx.setTargetSqlSchemaPath(PathProvider.getSource(conf, pool, tableConf, "target.sql"));	
				ctx.setAvroSchemaPath(PathProvider.getSource(conf, pool, tableConf, "avsc"));
//				setSqlInsert(conf, pool, ctx);
				if (isTableValid(conf, ctx)) {
					list.add(ctx);
				}
			}
			pool.setTables(list);
		}
	}

	private boolean isTableValid(DbImp_ConsoleConfig conf, AvroTableContext ctx) {
		if (!ctx.isEnabled()) {
			logger.info(LOG_HEADER + "task not enabled:[" + ctx.getSignature() + "]");
			return false;
		}
//		if (Files.exists(ctx.getAvroPath())) {
//			if (conf.isOverwriteTargetFile()) {
//				logger.warn(LOG_HEADER + "file avro already exists, will be overwritten - file:[" + ctx.getAvroPath() + "]");
//				return true;
//			} else {
//				logger.warn(LOG_HEADER + "task skipped because cannot overwrite avro file that already exists - file:[" + ctx.getAvroPath() + "]");
//				return false;
//			}
//		}
		return true;
	}
}
