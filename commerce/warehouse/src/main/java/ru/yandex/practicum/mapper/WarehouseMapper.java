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

    default AddressDto createAddressFromValue(String addressValue) {
        AddressDto dto = new AddressDto();
        dto.setCountry(addressValue);
        dto.setCity(addressValue);
        dto.setStreet(addressValue);
        dto.setHouse(addressValue);
        dto.setFlat(addressValue);
        return dto;
    }

    private AddressDto createDefaultAddress() {
        return createAddressFromValue("ADDRESS_1");
    }
}