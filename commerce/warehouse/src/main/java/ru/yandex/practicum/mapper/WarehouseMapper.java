package ru.yandex.practicum.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.Named;
import ru.yandex.practicum.dto.warehouse.AddressDto;
import ru.yandex.practicum.entity.WarehouseAddress;

@Mapper(componentModel = "spring")
public interface WarehouseMapper {

    @Named("toDtoWithDefault")
    default AddressDto toDto(WarehouseAddress address) {
        if (address == null) {
            return createDefaultAddress();
        }
        return mapToDto(address);
    }

    AddressDto mapToDto(WarehouseAddress address);

    private AddressDto createDefaultAddress() {
        // Теперь не генерируем случайный адрес, а возвращаем фиксированный
        // или используем переданный из сервиса
        AddressDto dto = new AddressDto();
        dto.setCountry("DEFAULT_ADDRESS");
        dto.setCity("DEFAULT_ADDRESS");
        dto.setStreet("DEFAULT_ADDRESS");
        dto.setHouse("DEFAULT_ADDRESS");
        dto.setFlat("DEFAULT_ADDRESS");
        return dto;
    }
}