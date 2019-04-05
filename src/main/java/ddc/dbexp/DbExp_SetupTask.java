package ddc.dbexp;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import ddc.dbio.AvroTableContext;
import ddc.dbio.PathProvider;
import ddc.support.task.Task;
import ddc.support.task.TaskException;
import ddc.support.util.FileUtil;
import ddc.support.util.LogConsole;
import ddc.support.util.LogListener;
import ddc.task.model.TableConfig;
import ddc.task.model.TablePool2Config;

public class DbExp_SetupTask extends Task {
	private final static LogListener logger = new LogConsole(DbExp_SetupTask.class);
	private static final String LOG_HEADER = "Startup - ";

	public void doRun() {
		DbExp_ConsoleConfig conf = (DbExp_ConsoleConfig) get(DbExp_ConsoleConfig.class);
		this.set("DBIOConfig", conf);
		
		if (!FileUtil.isReadbleFolder(conf.getTargetFolder())) {
			throw new TaskException(LOG_HEADER + "target folder is not valid :[" + conf.getTargetFolder() + "]");
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
						logger.info(LOG_HEADER + "Table is NOT enabled:[" + tableConf.getTable() + "]");		
					}
 				}
			}
		}
		
		List<TablePool2Config> pools = conf.getEnabledTablePoolList();
		for (TablePool2Config pool : pools) {
			List<AvroTableContext> list = new ArrayList<>();
			for (TableConfig tableConf : pool.getTables()) {
				AvroTableContext ctx = new AvroTableContext(tableConf);
				ctx.setSignature(PathProvider.getSignature(conf, pool, tableConf));
				ctx.setAvroPath(PathProvider.getAvroTarget(conf, pool, tableConf));
				ctx.setReportPath(Paths.get(ctx.getAvroPath().toString() + ".report"));
				ctx.setSourceSqlSchemaPath(PathProvider.getTarget(conf, pool, tableConf, "source.sql"));
				ctx.setTargetSqlSchemaPath(PathProvider.getTarget(conf, pool, tableConf, "target.sql"));	
				ctx.setAvroSchemaPath(PathProvider.getTarget(conf, pool, tableConf, "target.avsc"));
				if (StringUtils.isNotBlank(tableConf.getSelectScriptFile())) {
					Path path = Paths.get(tableConf.getSelectScriptFile());
					if (!Files.exists(path)) {
						throw new TaskException(LOG_HEADER + "Select script file not found - file:[" + path + "]");
					}
					ctx.setSelectScriptPath(path);
				}
				if (isTableValid(conf, ctx)) {
					list.add(ctx);
				}
			}
			pool.setTables(list);
		}
	}



	private boolean isTableValid(DbExp_ConsoleConfig conf, AvroTableContext ctx) {
		if (!ctx.isEnabled()) {
			logger.info(LOG_HEADER + "task not enabled:[" + ctx.getSignature() + "]");
			return false;
		}
		if (Files.exists(ctx.getAvroPath())) {
			if (conf.isOverwriteTargetFile()) {
				logger.warn(LOG_HEADER + "file avro already exists, will be overwritten - file:[" + ctx.getAvroPath() + "]");
				return true;
			} else {
				logger.warn(LOG_HEADER + "task skipped because cannot overwrite avro file that already exists - file:[" + ctx.getAvroPath() + "]");
				return false;
			}
		}
		return true;
	}
}
