package ddc.dbimp;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.commons.lang3.tuple.Triple;

import ddc.dbio.SqlAvroTypeConversion;
import ddc.dbio.SqlAvroTypeConversion.AvroManagedType;
import ddc.support.jdbc.schema.LiteDbColumn;
import ddc.support.jdbc.schema.LiteDbTable;
import ddc.support.util.Chronometer;
import ddc.support.util.LogConsole;
import ddc.support.util.LogListener;
import ddc.support.util.Statistics;

public class AvroToSql {
	private final static LogListener logger = new LogConsole(AvroToSql.class);
	private static final String LOG_HEADER = "Insert table - ";

	public void execute(Statistics stats, Path srcAvro, Connection trgConn, LiteDbTable targetTable, int batchSize) throws Exception {
		final Chronometer chron = new Chronometer(5 * 1000);
		PreparedStatement trgStatement = null;
		DataFileReader<GenericRecord> dataFileReader = null;
		DatumReader<GenericRecord> datumReader = null;
		List<Triple<JDBCType, Schema.Type, AvroManagedType>> triples = null;
		SqlAvroTypeConversion avroConv = new SqlAvroTypeConversion();
		try {
			//source avro
			logger.info(LOG_HEADER + "selecting data - source file:[" + srcAvro + "]");
			datumReader = new GenericDatumReader<GenericRecord>();
			dataFileReader = new DataFileReader<GenericRecord>(srcAvro.toFile(), datumReader);
			//target connection
			String sqlInsert = targetTable.buildInsertInto("\"", "\"");
			logger.info(LOG_HEADER + "building template to insert data - target sql:[" + sqlInsert + "]  batchSize:[" + batchSize + "]");
			trgStatement = trgConn.prepareStatement(sqlInsert);
			//loop
			int recCounter = 0;
			GenericRecord record = null;
			while (dataFileReader.hasNext()) {
				record = dataFileReader.next(record);
				if (recCounter == 1)
					logger.debug(LOG_HEADER + record);
				recCounter++;
				trgStatement.clearParameters();
				if (triples==null)
					triples = avroConv.buildTypes(record.getSchema(), targetTable);
				for (LiteDbColumn c : targetTable.getColumns()) {
					Object value = record.get(c.getName());
					avroConv.setSqlField(triples.get(c.getIndex()-1), c.getIndex(), trgStatement, value);
				}
				int[] affected = addAndExecuteBatch(trgStatement, batchSize, recCounter);
				updateStats(stats, affected);
				if (chron.isCountdownCycle())
					logger.debug(LOG_HEADER + "executeBatch - " + targetTable.getTableName() + " at line#:[" + recCounter + "]");
			}
			int[] affected = executeBatch(trgStatement);
			updateStats(stats, affected);
			logger.info(LOG_HEADER + targetTable.getTableName() + " " + stats.toString());
		} finally {
			if (dataFileReader != null)
				dataFileReader.close();
			if (trgStatement != null)
				trgStatement.close();
		}
	}

	private int[] addAndExecuteBatch(PreparedStatement stat, int batchSize, int recCounter) throws SQLException {
		stat.addBatch();
		if (recCounter % batchSize == 0) {
			int[] affected = executeBatch(stat);
			return affected;
		}
		return new int[] {};
	}

	private int[] executeBatch(PreparedStatement stat) throws SQLException {
		int[] affected = stat.executeBatch();
		stat.getConnection().commit();
		return affected;
	}

	private void updateStats(Statistics stats, int[] batchProcessed) {
		stats.itemsProcessed += batchProcessed.length;
		for (int i = 0; i < batchProcessed.length; i++) {
			stats.itemsAffected += batchProcessed[i];
			stats.itemsFailed += batchProcessed[i] == 0 ? 1 : 0;
		}
	}
}
