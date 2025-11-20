package ru.yandex.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import ru.yandex.practicum.entity.ScenarioCondition;
import ru.yandex.practicum.entity.ScenarioConditionId;

import java.util.List;

public interface ScenarioConditionRepository extends JpaRepository<ScenarioCondition, ScenarioConditionId> {

    List<ScenarioCondition> findByScenarioId(Long scenarioId);

    @Query("SELECT sc FROM ScenarioCondition sc " +
            "JOIN FETCH sc.sensor " +
            "JOIN FETCH sc.condition " +
            "WHERE sc.id.scenarioId = :scenarioId")
    List<ScenarioCondition> findWithAssociationsByScenarioId(@Param("scenarioId") Long scenarioId);

    @Modifying
    @Query("DELETE FROM ScenarioCondition sc WHERE sc.id.scenarioId = :scenarioId")
    void deleteByScenarioId(@Param("scenarioId") Long scenarioId);

    default List<ScenarioCondition> findWithAssociationsByScenarioIdSafe(Long scenarioId) {
        if (scenarioId == null) {
            return List.of();
        }
        return findWithAssociationsByScenarioId(scenarioId);
    }
}