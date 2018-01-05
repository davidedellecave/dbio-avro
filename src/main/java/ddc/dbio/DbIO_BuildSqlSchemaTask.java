package ddc.dbio;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.apache.avro.Schema;

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

	@SuppressWarnings("unchecked")
	private void executeTablePoll(TablePool2Config pool, int overrideMaxRows) throws ClassNotFoundException, SQLException, AvroConversionException, IOException {
		//
		logger.info(LOG_HEADER + "Building schema...");
		JdbcConnectionFactory fact = pool.getJdbcFactory();
		LiteDb db = LiteDb.build(fact);
		for (AvroTableContext tableCtx : (List<AvroTableContext>) pool.getTables()) {
			List<LiteDbTable> dbTables = db.findByTable(tableCtx.getTable());
			if (dbTables.size() >= 1) {
				LiteDbTable dbTable = dbTables.get(0);

				tableCtx.setDbTable(dbTable);

				String sqlSelect = getSqlSelect(pool, tableCtx, overrideMaxRows);
				tableCtx.setSqlSelect(sqlSelect);

				SqlAvroTypeConversion conv = new SqlAvroTypeConversion();
				Schema avroSchema = conv.buildAvroSchema(dbTable);
				tableCtx.setAvroSchema(avroSchema);
			} else {
				throw new TaskException(LOG_HEADER + "Table not found - table:[" + tableCtx.getTable() + "]");
			}
		}
	}

	private String getSqlSelect(TablePool2Config pool, AvroTableContext ctx, int overrideMaxRows) {
		// one row
		JdbcConnectionFactory factory = pool.getJdbcFactory();
		// rows
		String selectRows = "";
		if (overrideMaxRows > 0) {
			logger.info(LOG_HEADER + "all queries are limited - maxrows:[" + overrideMaxRows + "]");
			selectRows = factory.getSqlLimit(ctx.getTable(), ctx.getColumns(), overrideMaxRows);
		} else {
			if (ctx.getMaxrows() > 0) {
				selectRows = factory.getSqlLimit(ctx.getTable(), ctx.getColumns(), ctx.getMaxrows());
			} else {
				selectRows = "SELECT $COLUMNS FROM $TABLE".replace("$COLUMNS", ctx.getColumns()).replace("$TABLE", ctx.getTable());
			}
		}
		return selectRows;
	}
}
