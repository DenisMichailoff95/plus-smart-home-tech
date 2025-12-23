package ru.yandex.practicum.dto.common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PageResponse<T> {
    private List<T> content;
    private int pageNumber;
    private int pageSize;
    private long totalElements;
    private int totalPages;
    private boolean last;
    private List<SortInfo> sort;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SortInfo {
        private String property;
        private String direction;
    }
}