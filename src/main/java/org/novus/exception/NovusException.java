package org.novus.exception;

/**
 * 
 * Exception to be thrown from novus for library consumers
 * 
 * @author Manjeet Duhan
 * @date Aug 19, 2018
 *
 */
public class NovusException extends RuntimeException {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  public NovusException() {
    super();
  }

  public NovusException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

  public NovusException(String message, Throwable cause) {
    super(message, cause);
  }

  public NovusException(String message) {
    super(message);
  }

  public NovusException(Throwable cause) {
    super(cause);
  }

}
