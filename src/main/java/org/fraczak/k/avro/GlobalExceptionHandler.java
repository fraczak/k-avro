package org.fraczak.k.avro;

import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;
import java.util.logging.Logger;

import org.springframework.http.HttpStatus;

@RestControllerAdvice
public class GlobalExceptionHandler {

    public static String printException(Exception ex) {
        StackTraceElement origin = ex.getStackTrace()[0];  // First element = origin of the exception
        String msg = ex.getMessage();
        String file = origin.getFileName();
        int line = origin.getLineNumber();
        String method = origin.getMethodName();
        String className = origin.getClassName();

        return String.format("%s -- %s.%s(%s:%d)%n",
            msg, className, method, file, line);
    }

    private static final Logger logger = Logger.getLogger(CsmToAvroController.class.getName());

    // @ExceptionHandler(IllegalArgumentException.class)
    // @ResponseStatus(HttpStatus.BAD_REQUEST)
    // public Map<String, String> handleIllegalArgument(IllegalArgumentException ex) {
    //     logger.warning(printException(ex));
    //     return Map.of("error", ex.getMessage());
    // }

    @ExceptionHandler(Exception.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public Map<String, String> handleException(Exception ex) {
        logger.warning(printException(ex));
        return Map.of("error", ex.getMessage());
    }

    // You can add more handlers here for different exceptions
}
