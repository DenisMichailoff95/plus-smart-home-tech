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
        AddressDto dto = new AddressDto();
        dto.setCountry("ADDRESS_1");
        dto.setCity("ADDRESS_1");
        dto.setStreet("ADDRESS_1");
        dto.setHouse("ADDRESS_1");
        dto.setFlat("ADDRESS_1");
        return dto;
    }
}