package ch.bbw.m321.kafkasample.dto;

import java.time.LocalDateTime;
import java.util.UUID;

public record StructuredMessage(UUID id, String message, LocalDateTime timestamp) {}
