package ddc.dbexp;

import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;

import ddc.dbio.AvroTableContext;
import ddc.support.jack.JackUtil;
import ddc.support.task.Task;
import ddc.support.util.LogConsole;
import ddc.support.util.LogListener;
import ddc.task.model.TablePool2Config;

public class DbExp_WriteSchemaTask extends Task {
	private final static LogListener logger = new LogConsole(DbExp_WriteSchemaTask.class);
	
	private static final String LOG_HEADER = "Building sql script - ";

	@Override
	public void doRun() throws Exception {
		DbExp_ConsoleConfig conf = (DbExp_ConsoleConfig) get(DbExp_ConsoleConfig.class);
		List<TablePool2Config> pools = conf.getEnabledTablePoolList();
		for (TablePool2Config pool : pools) {
			executeTablePoll(conf, pool);
		}
	}

	@SuppressWarnings("unchecked")
	private void executeTablePoll(DbExp_ConsoleConfig conf, TablePool2Config pool) throws SQLException, ClassNotFoundException, IOException {
		//
		FileOutputStream outStream = null;
		try {
			for (AvroTableContext tableCtx : (List<AvroTableContext>) pool.getTables()) {
				//source
				logger.info(LOG_HEADER + tableCtx.getSignature() + " - source schema file:[" + tableCtx.getSourceSqlSchemaPath().toString() + "]");
				String createTableString = tableCtx.getDbTable().buildCreateTable(conf.getSourceSqlTypeMap());
				logger.debug(LOG_HEADER + "writing sql for source db - sql:[" + createTableString + "]");
				outStream = new FileOutputStream(tableCtx.getSourceSqlSchemaPath().toString());
				outStream.write(createTableString.getBytes());
				outStream.close();
				//target
				logger.info(LOG_HEADER + tableCtx.getSignature() + " - target schema file:[" + tableCtx.getTargetSqlSchemaPath().toString() + "]");
				createTableString = tableCtx.getDbTable().buildCreateTable(conf.getTargetSqlTypeMap());
				logger.debug(LOG_HEADER + "writing sql for target db - sql:[" + createTableString + "]");
				outStream = new FileOutputStream(tableCtx.getTargetSqlSchemaPath().toString());
				outStream.write(createTableString.getBytes());
				outStream.close();	
				//avro schema
				logger.info(LOG_HEADER + tableCtx.getSignature() + " - avro schema file:[" + tableCtx.getAvroSchemaPath().toString() + "]");
				logger.debug(LOG_HEADER + "writing avro schema - avsc:[" + tableCtx.getAvroSchema().toString() + "]");
				outStream = new FileOutputStream(tableCtx.getAvroSchemaPath().toString());
				
				outStream.write(tableCtx.getAvroSchema().toString(true).getBytes());
				outStream.close();	
			}				
		} finally {
			if (outStream != null)
				outStream.close();
		}		
	}

}
