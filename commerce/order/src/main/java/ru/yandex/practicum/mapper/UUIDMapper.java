package ru.yandex.practicum.mapper;

import org.mapstruct.Mapper;

import java.util.UUID;

@Mapper(componentModel = "spring")
public interface UUIDMapper {

    default String uuidToString(UUID uuid) {
        return uuid != null ? uuid.toString() : null;
    }

    default UUID stringToUuid(String str) {
        return str != null ? UUID.fromString(str) : null;
    }
}