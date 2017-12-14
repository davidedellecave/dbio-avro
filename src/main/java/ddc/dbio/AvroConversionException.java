package ddc.dbio;

public class AvroConversionException extends Exception {
	private static final long serialVersionUID = -3532929207516055321L;

	public AvroConversionException(String message) {
        super(message);
    }
    
    public AvroConversionException(Throwable cause) {
        super(cause);
    }
    
    public AvroConversionException(String message, Throwable cause) {
        super(message, cause);
    }
}
