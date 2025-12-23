package ru.yandex.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import ru.yandex.practicum.entity.ScenarioAction;
import ru.yandex.practicum.entity.ScenarioActionId;

import java.util.List;

@Repository
public interface ScenarioActionRepository extends JpaRepository<ScenarioAction, ScenarioActionId> {

    List<ScenarioAction> findByScenarioId(Long scenarioId);

    @Query("SELECT sa FROM ScenarioAction sa " +
            "JOIN FETCH sa.sensor " +
            "JOIN FETCH sa.action " +
            "WHERE sa.id.scenarioId = :scenarioId")
    List<ScenarioAction> findWithAssociationsByScenarioId(@Param("scenarioId") Long scenarioId);

    @Modifying
    @Query("DELETE FROM ScenarioAction sa WHERE sa.id.scenarioId = :scenarioId")
    void deleteByScenarioId(@Param("scenarioId") Long scenarioId);

    default List<ScenarioAction> findWithAssociationsByScenarioIdSafe(Long scenarioId) {
        if (scenarioId == null) {
            return List.of();
        }
        return findWithAssociationsByScenarioId(scenarioId);
    }
}