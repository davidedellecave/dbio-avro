package ddc.dbio;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;

import ddc.support.jdbc.JdbcConnectionFactory;
import ddc.support.jdbc.schema.LiteDb;
import ddc.support.jdbc.schema.LiteDbTable;
import ddc.support.task.Task;
import ddc.support.task.TaskException;
import ddc.support.util.LogConsole;
import ddc.support.util.LogListener;
import ddc.task.model.TablePool2Config;

public class DbIO_BuildSqlSchemaTask extends Task {
	private final static LogListener logger = new LogConsole(DbIO_BuildSqlSchemaTask.class);
	private static final String LOG_HEADER = "Building schema - ";

	@Override
	public void doRun() throws Exception {
		DbIO_Config conf = (DbIO_Config) this.get("DBIOConfig");
		List<TablePool2Config> pools = conf.getEnabledTablePoolList();
		for (TablePool2Config pool : pools) {
			executeTablePoll(pool, conf.getOverrideMaxRows());
		}
	}

	// private boolean FORCE_IF_TABLE_NOTFOUND=true;
	@SuppressWarnings("unchecked")
	private void executeTablePoll(TablePool2Config pool, int overrideMaxRows) throws Exception {
		for (AvroTableContext tableCtx : (List<AvroTableContext>) pool.getTables()) {
			LiteDb db = buildSchema(pool, tableCtx);
			List<LiteDbTable> dbTables = db.findByTable(tableCtx.getTable());
			if (dbTables.size() > 0) {
				LiteDbTable dbTable = dbTables.get(0);

				tableCtx.setDbTable(dbTable);

				String sqlSelect = getSqlSelect(pool, tableCtx, overrideMaxRows);
				logger.info(LOG_HEADER + "Select:[" + sqlSelect + "]");
				tableCtx.setSqlSelect(sqlSelect);

				SqlAvroTypeConversion conv = new SqlAvroTypeConversion();
				Schema avroSchema = conv.buildAvroSchema(dbTable);
				tableCtx.setAvroSchema(avroSchema);
			} else {
				throw new TaskException(LOG_HEADER + "Table not found - table:[" + tableCtx.getTable() + "]");
			}
		}
	}

	private LiteDb buildSchema(TablePool2Config pool, AvroTableContext tableCtx) throws Exception {
		logger.info(LOG_HEADER + "Building schema...");
		JdbcConnectionFactory jdbcFactory = pool.getJdbcFactory();
		String tableName = tableCtx.getTable();
		String schemaName = !StringUtils.isBlank(pool.getSchema()) ? pool.getSchema() : null;
		LiteDb db = new LiteDb(jdbcFactory.getSchemaBuilder());
		db.build(jdbcFactory, schemaName, tableName);
		logger.info(LOG_HEADER + "Building schema. done");
		return db;
	}

	private String getSqlSelect(TablePool2Config pool, AvroTableContext tableCtx, int overrideMaxRows) throws IOException {
		// one row
		JdbcConnectionFactory factory = pool.getJdbcFactory();
		// rows
		String tableName = tableCtx.getTable();
		if (StringUtils.isNotBlank(pool.getSchema())) {
			tableName = pool.getSchema() + "." + tableCtx.getTable();
		}
		String selectRows = "";
		if (tableCtx.getSelectScriptPath() != null) {
			Path path = tableCtx.getSelectScriptPath();
			selectRows = new String(Files.readAllBytes(path));
		} else {
			if (overrideMaxRows > 0) {
				logger.info(LOG_HEADER + "all selects are limited - maxrows:[" + overrideMaxRows + "]");
				selectRows = factory.getSqlLimit(tableName, tableCtx.getColumns(), overrideMaxRows);
			} else {
				if (tableCtx.getMaxrows() > 0) {
					logger.info(LOG_HEADER + "select is limited - maxrows:[" + tableCtx.getMaxrows() + "]");
					selectRows = factory.getSqlLimit(tableName, tableCtx.getColumns(), tableCtx.getMaxrows());
				} else {
					selectRows = "SELECT $COLUMNS FROM $TABLE".replace("$COLUMNS", tableCtx.getColumns()).replace("$TABLE", tableName);
				}
			}
		}
		return selectRows;
	}

	// private String parseWhereCondition(String where) {
	// String s = where;
	// if (where.contains("days")) {
	// s = StringUtils.substringBetween(s, "days(", ")").trim();
	// String[] toks = s.split(",");
	// if (toks.length==2) {
	// int days = Integer.parseInt(toks[0].trim());
	// String pattern = StringUtils.substringBetween(s, "\"");
	// }
	// }
	// return s;
	//
	// }
}
