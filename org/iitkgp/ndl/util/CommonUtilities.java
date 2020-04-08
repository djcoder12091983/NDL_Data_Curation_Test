package org.iitkgp.ndl.util;

import java.math.BigDecimal;

/**
 * Common utilities
 * @author Debasis
 */
public class CommonUtilities {

	static final long KB = 1024;
	static final long MB = KB * 1024;
	static final long GB = MB * 1024;
	
	/**
	 * Prints duration message on std-out
	 * @param milliseconds given milliseconds
	 * @return returns corresponding message
	 */
	public static String durationMessage(long milliseconds) {
		long hours = 0;
		long minutes = 0;
		BigDecimal divides[] = new BigDecimal(milliseconds/1000).divideAndRemainder(new BigDecimal(60));
		long seconds = divides[1].longValue();
		if(divides[0].longValue() > 0) {
			divides = divides[0].divideAndRemainder(new BigDecimal(60));
			minutes = divides[1].longValue();
			if(divides[0].longValue() > 0) {
				hours = divides[0].longValue();
			}
		}
		
		StringBuilder message = new StringBuilder("Time taken : ");
		if(hours > 0) {
			message.append(hours).append(" Hour").append(hours > 1 ? "s" : "").append(" ");
		}
		if(minutes > 0) {
			message.append(minutes).append(" Minute").append(minutes > 1 ? "s" : "").append(" ");
		}
		if(seconds > 0) {
			message.append(seconds).append(" Second").append(seconds > 1 ? "s" : "");
		} else {
			message.append("0 second");
		}
		
		return message.toString().trim();
	}
	
	/**
	 * Gets exception detail from a given exception (typically from stack-trace) 
	 * @param ex given exception message
	 * @return returns exception detail message
	 */
	public static String exceptionDetail(Exception ex) {
		StringBuilder error = new StringBuilder("Exception in thread \"");
		// initial detail
		error.append(Thread.currentThread().getName()).append("\" ");
		error.append(Thread.currentThread().getName()).append(": ");
		error.append(ex.getMessage()).append(NDLDataUtils.NEW_LINE);
		
		StackTraceElement errors[] = ex.getStackTrace();
		// more detail
		for(StackTraceElement err : errors) {
			int ln = err.getLineNumber();
			error.append("\tat ").append(err.getClassName()).append('.').append(err.getMethodName()).append('(')
					.append(ln > 0 ? (err.getFileName() + ":" + ln) : "Native Method").append(')')
					.append(NDLDataUtils.NEW_LINE);
		}
		
		return error.toString();
	}
	
	/**
	 * Converts bytes message (KB, MB, GB etc.)
	 * @param bytes given bytes to convert
	 * @return returns bytes message
	 */
	public static String bytesMessage(long bytes) {
		if(bytes > GB) {
			return bytes / GB  + "GB";
		} else if(bytes > MB) {
			return bytes / MB + " MB";
		} else if(bytes > KB) {
			return bytes / KB + " KB";
		} else {
			return bytes  + " bytes";
		}
	}
}