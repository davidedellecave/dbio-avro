package ddc.dbio;

import java.nio.file.Path;

import org.apache.avro.Schema;

import ddc.support.jdbc.schema.LiteDbTable;
import ddc.task.model.TableConfig;

public class AvroTableContext extends TableConfig {
	private String signature;
	private LiteDbTable dbTable;
	private Schema avroSchema;
	private Path avroPath;
	private Path avroSchemaPath;
	private Path sourceSqlSchemaPath;
	private Path targetSqlSchemaPath;

	private Path reportPath;
	private String sqlSelect;

	public AvroTableContext(TableConfig tc) {
		this.setEnabled(tc.isEnabled());
		this.setTable(tc.getTable());
		this.setColumns(tc.getColumns());
		this.setMaxrows(tc.getMaxrows());
	}

	public AvroTableContext clone() {
		AvroTableContext t = new AvroTableContext(super.clone());
		t.setAvroPath(this.avroPath);
		t.setAvroSchema(this.getAvroSchema());
		t.setColumns(this.getColumns());
		t.setDbTable(this.getDbTable());
		t.setReportPath(this.getReportPath());
		t.setSignature(this.getSignature());
		t.setSourceSqlSchemaPath(this.getSourceSqlSchemaPath());
		t.setSqlSelect(this.getSqlSelect());
		t.setTargetSqlSchemaPath(this.getTargetSqlSchemaPath());
		return t;
	}

	public String getSignature() {
		return signature;
	}

	public void setSignature(String signature) {
		this.signature = signature;
	}

	public LiteDbTable getDbTable() {
		return dbTable;
	}

	public void setDbTable(LiteDbTable dbTable) {
		this.dbTable = dbTable;
	}

	public Schema getAvroSchema() {
		return avroSchema;
	}

	public void setAvroSchema(Schema avroSchema) {
		this.avroSchema = avroSchema;
	}

	public Path getAvroPath() {
		return avroPath;
	}

	public void setAvroPath(Path avroPath) {
		this.avroPath = avroPath;
	}

	public Path getAvroSchemaPath() {
		return avroSchemaPath;
	}

	public void setAvroSchemaPath(Path avroSchemaPath) {
		this.avroSchemaPath = avroSchemaPath;
	}

	public Path getSourceSqlSchemaPath() {
		return sourceSqlSchemaPath;
	}

	public void setSourceSqlSchemaPath(Path sourceSqlSchemaPath) {
		this.sourceSqlSchemaPath = sourceSqlSchemaPath;
	}

	public Path getTargetSqlSchemaPath() {
		return targetSqlSchemaPath;
	}

	public void setTargetSqlSchemaPath(Path targetSqlSchemaPath) {
		this.targetSqlSchemaPath = targetSqlSchemaPath;
	}

	public Path getReportPath() {
		return reportPath;
	}

	public void setReportPath(Path reportPath) {
		this.reportPath = reportPath;
	}

	public String getSqlSelect() {
		return sqlSelect;
	}

	public void setSqlSelect(String sqlSelect) {
		this.sqlSelect = sqlSelect;
	}

}
