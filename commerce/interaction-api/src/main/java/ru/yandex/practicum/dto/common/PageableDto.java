package ru.yandex.practicum.dto.common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PageableDto {
    private Integer page = 0;
    private Integer size = 20;
    private List<String> sort;
}