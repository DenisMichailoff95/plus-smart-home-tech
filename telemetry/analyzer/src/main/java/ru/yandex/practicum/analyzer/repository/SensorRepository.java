package ru.yandex.practicum.analyzer.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import ru.yandex.practicum.analyzer.model.Sensor;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

public interface SensorRepository extends JpaRepository<Sensor, String> {

    boolean existsByIdInAndHubId(Collection<String> ids, String hubId);

    Optional<Sensor> findByIdAndHubId(String id, String hubId);

    void deleteByIdAndHubId(String id, String hubId);

    // Добавляем метод для поиска всех датчиков по списку ID и hubId
    @Query("SELECT s FROM Sensor s WHERE s.id IN :ids AND s.hubId = :hubId")
    List<Sensor> findByIdInAndHubId(@Param("ids") Collection<String> ids, @Param("hubId") String hubId);
}