package ru.yandex.practicum.dto.common;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.http.HttpStatus;

import java.util.ArrayList;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ErrorResponse {
    private Cause cause;
    private List<StackTraceElement> stackTrace;
    private String httpStatus;
    private String userMessage;
    private String message;
    private List<Cause> suppressed;
    private String localizedMessage;

    public ErrorResponse(HttpStatus httpStatus, String message, String userMessage) {
        this.httpStatus = httpStatus.toString();
        this.message = message;
        this.userMessage = userMessage;
        this.stackTrace = new ArrayList<>();
        this.suppressed = new ArrayList<>();
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Cause {
        private List<StackTraceElement> stackTrace;
        private String message;
        private String localizedMessage;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class StackTraceElement {
        private String classLoaderName;
        private String moduleName;
        private String moduleVersion;
        private String methodName;
        private String fileName;
        private Integer lineNumber;
        private String className;
        private Boolean nativeMethod;
    }
}