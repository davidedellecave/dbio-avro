package ddc.dbexp;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;

import ddc.dbio.AvroConversionException;
import ddc.dbio.AvroTableContext;
import ddc.dbio.SqlAvroTypeConversion;
import ddc.support.jdbc.JdbcConnectionFactory;
import ddc.support.util.Chronometer;
import ddc.support.util.LogConsole;
import ddc.support.util.LogListener;
import ddc.support.util.Statistics;
import ddc.task.model.TablePool2Config;

public class SqlToAvro {
	private static final String LOG_HEADER = "Writing avro - ";
	private static boolean DEBUG = false;
	private final static LogListener logger = new LogConsole(SqlToAvro.class);

	public void execute(Statistics stats, TablePool2Config pool, AvroTableContext tableCtx, FileOutputStream avroStream, CodecFactory codecFactory)
			throws JsonGenerationException, JsonMappingException, ClassNotFoundException, SQLException, IOException, AvroConversionException {
		if (DEBUG) {
			// doDebugExecute(stats, pool, tableCtx, avroStream, codecFactory);
		} else {
			exAvroData(stats, pool, tableCtx, avroStream, codecFactory);
		}
	}

	// private void doDebugExecute(Statistics stats, TablePool2Config pool,
	// AvroTableContext tblCtx, FileOutputStream avroStream, CodecFactory
	// codecFactory) {
	// LiteDbTable sqlSchema = null;
	// try {
	// JdbcConnectionFactory factory = pool.getJdbcFactory();
	// try (Connection sqlConnection = factory.createConnection()) {
	// sqlSchema = LiteDbTable.build(sqlConnection, tblCtx.getSqlSelectOneRow());
	// }
	// String selectRows = tblCtx.getSqlSelect();
	// String selectOneRow = tblCtx.getSqlSelectOneRow();
	// for (LiteDbColumn col : sqlSchema.getColumns()) {
	// String newSelectRows = selectRows.replace("*", col.getName());
	// String newSelectOneRow = selectOneRow.replace("*", col.getName());
	// logger.debug(LOG_HEADER + newSelectRows);
	// AvroTableContext newTblCtx = tblCtx.clone();
	// newTblCtx.setSqlSelectOneRow(newSelectOneRow);
	// newTblCtx.setSqlSelect(selectRows);
	// exAvroData(stats, pool, newTblCtx, avroStream, codecFactory);
	// }
	// } catch (Throwable e) {
	// logger.error(sqlSchema.toString() + " - " + e.getMessage());
	// }
	// }

	private void exAvroData(Statistics stats, TablePool2Config pool, AvroTableContext tableCtx,  FileOutputStream avroStream, CodecFactory codecFactory) throws ClassNotFoundException, SQLException, JsonGenerationException, JsonMappingException, IOException, AvroConversionException {
		logger.info(LOG_HEADER + "Starting table export... - " + tableCtx.getSignature());		
		JdbcConnectionFactory factory =	pool.getJdbcFactory();		
		Connection sqlConnection = null;
		try {
			sqlConnection = factory.createConnection(); 
			writeAvro(stats, tableCtx, sqlConnection, avroStream, pool.getFetch(), codecFactory);			
		} finally {
			JdbcConnectionFactory.close(sqlConnection);			
		}
	}

	private final static int RS_TYPE = ResultSet.TYPE_FORWARD_ONLY;
	private final static int RS_CONCURRENCY = ResultSet.CONCUR_READ_ONLY;
	private final static int FETCH_SIZE = 1000;

	public void writeAvro(Statistics stats, AvroTableContext tableCtx, Connection sqlConnection, OutputStream avroStream, int fetch, CodecFactory avroCompressionCodec) throws IOException, SQLException, AvroConversionException {
		if (fetch <= 0)
			fetch = FETCH_SIZE;

		sqlConnection.setAutoCommit(false);
		DataFileWriter<GenericRecord> dataFileWriter = null;
		ResultSet rs = null;
		Statement sqlStatement = null;
		ResultSetMetaData meta = null;
		SqlAvroTypeConversion avroConv = new SqlAvroTypeConversion();
		// int rowCounter = 0;
		try {

			Schema avroSchema = tableCtx.getAvroSchema();

			DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(avroSchema);
			dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
			dataFileWriter.setCodec(avroCompressionCodec);
			GenericRecord record = new GenericData.Record(avroSchema);
			dataFileWriter.create(avroSchema, avroStream);
			stats.itemsFailed = 0;
			stats.itemsProcessed = 0;

			sqlStatement = sqlConnection.createStatement(RS_TYPE, RS_CONCURRENCY);
			sqlStatement.setFetchSize(fetch);

			logger.info(LOG_HEADER + "executing sql:[" + tableCtx.getSqlSelect() + "]...");
			rs = sqlStatement.executeQuery(tableCtx.getSqlSelect());

			meta = rs.getMetaData();
			logger.info(LOG_HEADER + "...");

			final Chronometer chron = new Chronometer(5 * 1000);
			while (rs.next()) {
				stats.itemsProcessed++;
				for (int i = 1; i <= meta.getColumnCount(); i++) {
					Object value = rs.getObject(i);
					avroConv.setAvroField(tableCtx.getDbTable(), record, meta.getColumnName(i), value);
				}
				dataFileWriter.append(record);
				if (chron.isCountdownCycle())
					logger.debug(LOG_HEADER + "table:[" + tableCtx.getTable() + "] at line#:[" + stats.itemsProcessed + "]");
			}
		} finally {
			if (rs != null && !rs.isClosed())
				rs.close();
			if (sqlStatement != null && !sqlStatement.isClosed())
				sqlStatement.close();
			if (dataFileWriter != null)
				dataFileWriter.close();
		}
	}
}
