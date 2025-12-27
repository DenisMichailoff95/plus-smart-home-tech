package ru.yandex.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.entity.WarehouseAddress;

@Repository
public interface WarehouseAddressRepository extends JpaRepository<WarehouseAddress, Long> {
}