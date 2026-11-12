package com.example.topicadmin.service;

import com.example.topicadmin.exception.ApiExceptions.BadRequestException;
import com.example.topicadmin.exception.ApiExceptions.ConflictException;
import com.example.topicadmin.exception.ApiExceptions.KafkaUnavailableException;
import com.example.topicadmin.exception.ApiExceptions.NotFoundException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.util.concurrent.ExecutionException;

/** Espera un resultado de AdminClient y traduce los errores de Kafka a errores HTTP. */
final class KafkaCalls {

    private KafkaCalls() {
    }

    static <T> T await(KafkaFuture<T> future) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaUnavailableException("Operacion interrumpida", e);
        } catch (ExecutionException e) {
            throw translate(e.getCause() != null ? e.getCause() : e);
        }
    }

    static RuntimeException translate(Throwable t) {
        if (t instanceof UnknownTopicOrPartitionException) {
            return new NotFoundException("El topico o la particion no existe");
        }
        if (t instanceof GroupIdNotFoundException) {
            return new NotFoundException("El consumer group no existe");
        }
        if (t instanceof GroupNotEmptyException || t instanceof UnknownMemberIdException) {
            return new ConflictException("El grupo tiene consumidores activos: detenerlos antes de modificar sus offsets");
        }
        if (t instanceof InvalidConfigurationException || t instanceof InvalidRequestException) {
            return new BadRequestException(t.getMessage());
        }
        if (t instanceof TimeoutException) {
            return new KafkaUnavailableException("Kafka no responde (verificar que el cluster este arriba)", t);
        }
        if (t instanceof RuntimeException r && r.getClass().getName().startsWith("com.example")) {
            return r;
        }
        return new KafkaUnavailableException("Error hablando con Kafka: " + t.getMessage(), t);
    }
}
