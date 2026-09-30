package br.com.freela.contrato.infrastructure.messaging;

import tools.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.UUID;

public record EventoEnvelope(UUID eventId, String eventType, UUID contratoId, Instant occurredAt,
                             String correlationId, JsonNode payload) {}