package ddc.dbexp;

import ddc.dbio.DbIO_Config;

public class DbExp_ConsoleConfig extends DbIO_Config {
	private String targetFolder = null;
	private String targetFileTemplate = "";
	private boolean deleteTargetFileOnError = true;
	private boolean overwriteTargetFile = false;
	
	//	
	public String getTargetFolder() {
		return targetFolder;
	}
	public void setTargetFolder(String targetFolder) {
		this.targetFolder = targetFolder;
	}
	public String getTargetFileTemplate() {
		return targetFileTemplate;
	}
	public void setTargetFileTemplate(String targetFileTemplate) {
		this.targetFileTemplate = targetFileTemplate;
	}
	public boolean isDeleteTargetFileOnError() {
		return deleteTargetFileOnError;
	}
	public void setDeleteTargetFileOnError(boolean deleteTargetFileOnError) {
		this.deleteTargetFileOnError = deleteTargetFileOnError;
	}
	public boolean isOverwriteTargetFile() {
		return overwriteTargetFile;
	}
	public void setOverwriteTargetFile(boolean overwriteTargetFile) {
		this.overwriteTargetFile = overwriteTargetFile;
	}
	

}
