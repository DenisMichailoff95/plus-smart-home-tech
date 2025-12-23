package ru.yandex.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import ru.yandex.practicum.entity.Scenario;

import java.util.List;
import java.util.Optional;

@Repository
public interface ScenarioRepository extends JpaRepository<Scenario, Long> {

    Optional<Scenario> findByHubIdAndName(String hubId, String name);

    List<Scenario> findByHubId(String hubId);

    @Query("SELECT s FROM Scenario s LEFT JOIN FETCH s.conditions WHERE s.hubId = :hubId")
    List<Scenario> findByHubIdWithConditions(@Param("hubId") String hubId);

    @Query("SELECT s FROM Scenario s LEFT JOIN FETCH s.actions WHERE s.hubId = :hubId")
    List<Scenario> findByHubIdWithActions(@Param("hubId") String hubId);
}