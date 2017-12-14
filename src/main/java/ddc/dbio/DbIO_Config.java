package ddc.dbio;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.avro.file.CodecFactory;

import ddc.support.jdbc.db.SqlTypeMap;
import ddc.task.model.TablePool2Config;

public class DbIO_Config {
	private SqlTypeMap sourceSqlTypeMap=null;
	private SqlTypeMap targetSqlTypeMap=null;
	private List<TablePool2Config> tablePoolList = null;
	private int overrideMaxRows = -1;	
	//<!-- none, snappy, bzip2 -->
	private String compressionMode = "none";
	
	public List<TablePool2Config> getEnabledTablePoolList() {
		return tablePoolList.stream().filter(x -> x.isEnabled()).collect(Collectors.toList());
	}
	
	public CodecFactory getAvroCodeFactory() {
		if ("bzip2".equals(getCompressionMode()))
			return CodecFactory.bzip2Codec();

		if ("snappy".equals(getCompressionMode()))
			return CodecFactory.snappyCodec();

		if ("deflate".equals(getCompressionMode()))
			return CodecFactory.deflateCodec(0);
				
		return CodecFactory.nullCodec();

	}
	
	public SqlTypeMap getSourceSqlTypeMap() {
		return sourceSqlTypeMap;
	}
	public void setSourceSqlTypeMap(SqlTypeMap sourceSqlTypeMap) {
		this.sourceSqlTypeMap = sourceSqlTypeMap;
	}
	public SqlTypeMap getTargetSqlTypeMap() {
		return targetSqlTypeMap;
	}
	public void setTargetSqlTypeMap(SqlTypeMap targetSqlTypeMap) {
		this.targetSqlTypeMap = targetSqlTypeMap;
	}
	public List<TablePool2Config> getTablePoolList() {
		return tablePoolList;
	}
	public void setTablePoolList(List<TablePool2Config> tablePoolList) {
		this.tablePoolList = tablePoolList;
	}
	public int getOverrideMaxRows() {
		return overrideMaxRows;
	}
	public void setOverrideMaxRows(int overrideMaxRows) {
		this.overrideMaxRows = overrideMaxRows;
	}
	public String getCompressionMode() {
		return compressionMode;
	}
	public void setCompressionMode(String compressionMode) {
		this.compressionMode = compressionMode;
	}
	
	
}
