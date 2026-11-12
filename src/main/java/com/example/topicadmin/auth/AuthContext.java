package com.example.topicadmin.auth;

import com.example.topicadmin.exception.ApiExceptions.ForbiddenException;
import com.example.topicadmin.exception.ApiExceptions.UnauthorizedException;

/**
 * Identidad de la request (headers que inyecta el BFF tras validar el JWT). Mismo contrato que los demás
 * servicios. Todo este servicio es solo para el taller: no se expone directo a internet.
 */
public record AuthContext(String userId, String role) {

    public static final String USER_ID_HEADER = "X-User-Id";
    public static final String ROLE_HEADER = "X-User-Role";
    public static final String ROLE_WORKSHOP_ADMIN = "WORKSHOP_ADMIN";

    public static AuthContext requireAdmin(String userId, String role) {
        if (userId == null || userId.isBlank()) {
            throw new UnauthorizedException("Falta el encabezado de identidad: " + USER_ID_HEADER);
        }
        if (!ROLE_WORKSHOP_ADMIN.equals(role)) {
            throw new ForbiddenException("Solo el administrador (WORKSHOP_ADMIN) puede usar este servicio");
        }
        return new AuthContext(userId, role);
    }
}
