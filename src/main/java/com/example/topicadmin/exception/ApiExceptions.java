package com.example.topicadmin.exception;

public final class ApiExceptions {

    private ApiExceptions() {
    }

    public static class UnauthorizedException extends RuntimeException {
        public UnauthorizedException(String m) { super(m); }
    }

    public static class ForbiddenException extends RuntimeException {
        public ForbiddenException(String m) { super(m); }
    }

    public static class NotFoundException extends RuntimeException {
        public NotFoundException(String m) { super(m); }
    }

    public static class ConflictException extends RuntimeException {
        public ConflictException(String m) { super(m); }
    }

    public static class BadRequestException extends RuntimeException {
        public BadRequestException(String m) { super(m); }
    }

    /** El clúster no responde (broker caído o sin conexión). */
    public static class KafkaUnavailableException extends RuntimeException {
        public KafkaUnavailableException(String m, Throwable cause) { super(m, cause); }
    }
}
