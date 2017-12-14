package ddc.dbimp;

import ddc.dbio.DbIO_Config;

public class DbImp_ConsoleConfig extends DbIO_Config {
	private String sourceFolder = null;
	private String sourceFileTemplate = "";
	private boolean truncateTargetTableOnStartup = false;
	private boolean truncateTargetTableOnError = true;
	
	public String getSourceFolder() {
		return sourceFolder;
	}
	public void setSourceFolder(String sourceFolder) {
		this.sourceFolder = sourceFolder;
	}
	public String getSourceFileTemplate() {
		return sourceFileTemplate;
	}
	public void setSourceFileTemplate(String sourceFileTemplate) {
		this.sourceFileTemplate = sourceFileTemplate;
	}
	public boolean isTruncateTargetTableOnStartup() {
		return truncateTargetTableOnStartup;
	}
	public void setTruncateTargetTableOnStartup(boolean truncateTargetTableOnStartup) {
		this.truncateTargetTableOnStartup = truncateTargetTableOnStartup;
	}
	public boolean isTruncateTargetTableOnError() {
		return truncateTargetTableOnError;
	}
	public void setTruncateTargetTableOnError(boolean truncateTargetTableOnError) {
		this.truncateTargetTableOnError = truncateTargetTableOnError;
	}

	
}
