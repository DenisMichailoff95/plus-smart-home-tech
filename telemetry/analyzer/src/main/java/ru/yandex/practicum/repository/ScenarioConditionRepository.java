package ru.yandex.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.entity.ScenarioCondition;
import ru.yandex.practicum.entity.ScenarioConditionId;

import java.util.List;

@Repository
public interface ScenarioConditionRepository extends JpaRepository<ScenarioCondition, ScenarioConditionId> {

    List<ScenarioCondition> findByIdScenarioId(Long scenarioId);

    @Query("SELECT sc FROM ScenarioCondition sc " +
            "JOIN FETCH sc.sensor " +
            "JOIN FETCH sc.condition " +
            "WHERE sc.id.scenarioId = :scenarioId")
    List<ScenarioCondition> findWithAssociationsByIdScenarioId(@Param("scenarioId") Long scenarioId);

    @Modifying
    @Query("DELETE FROM ScenarioCondition sc WHERE sc.id.scenarioId = :scenarioId")
    void deleteByIdScenarioId(@Param("scenarioId") Long scenarioId);
}