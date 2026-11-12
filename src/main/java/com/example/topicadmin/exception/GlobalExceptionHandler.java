package com.example.topicadmin.exception;

import com.example.topicadmin.exception.ApiExceptions.*;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(UnauthorizedException.class)
    ResponseEntity<Map<String, String>> unauthorized(UnauthorizedException e) {
        return body(HttpStatus.UNAUTHORIZED, e.getMessage());
    }

    @ExceptionHandler(ForbiddenException.class)
    ResponseEntity<Map<String, String>> forbidden(ForbiddenException e) {
        return body(HttpStatus.FORBIDDEN, e.getMessage());
    }

    @ExceptionHandler(NotFoundException.class)
    ResponseEntity<Map<String, String>> notFound(NotFoundException e) {
        return body(HttpStatus.NOT_FOUND, e.getMessage());
    }

    @ExceptionHandler(ConflictException.class)
    ResponseEntity<Map<String, String>> conflict(ConflictException e) {
        return body(HttpStatus.CONFLICT, e.getMessage());
    }

    @ExceptionHandler(BadRequestException.class)
    ResponseEntity<Map<String, String>> badRequest(BadRequestException e) {
        return body(HttpStatus.BAD_REQUEST, e.getMessage());
    }

    @ExceptionHandler(KafkaUnavailableException.class)
    ResponseEntity<Map<String, String>> unavailable(KafkaUnavailableException e) {
        return body(HttpStatus.SERVICE_UNAVAILABLE, e.getMessage());
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    ResponseEntity<Map<String, String>> invalid(MethodArgumentNotValidException e) {
        String msg = e.getBindingResult().getFieldErrors().stream()
                .map(f -> f.getField() + " " + f.getDefaultMessage()).findFirst().orElse("Solicitud invalida");
        return body(HttpStatus.BAD_REQUEST, msg);
    }

    private static ResponseEntity<Map<String, String>> body(HttpStatus status, String message) {
        return ResponseEntity.status(status).body(Map.of("error", message));
    }
}
