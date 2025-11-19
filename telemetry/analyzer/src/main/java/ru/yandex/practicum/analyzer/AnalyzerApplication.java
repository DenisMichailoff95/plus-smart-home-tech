package ru.yandex.practicum.analyzer;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.concurrent.*;

@SpringBootApplication
public class AnalyzerApplication {

    @Value("${threadPool.arrayBlockingQueue.capacity:10}")
    private int arrayBlockingQueueCapacity;

    @Value("${threadPool.corePoolSize:2}")
    private int threadPoolCorePoolSize;

    @Value("${threadPool.maximumPoolSize:4}")
    private int threadPoolMaximumPoolSize;

    @Value("${threadPool.keepAliveTime:60}")
    private long threadPoolKeepAliveTime;

    public static void main(String[] args) {
        SpringApplication.run(AnalyzerApplication.class, args);
    }

    @Bean
    public ExecutorService getExecutorService() {
        BlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(arrayBlockingQueueCapacity);
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(
                threadPoolCorePoolSize,
                threadPoolMaximumPoolSize,
                threadPoolKeepAliveTime,
                TimeUnit.SECONDS,
                queue,
                new ThreadPoolExecutor.CallerRunsPolicy() // Используем CallerRunsPolicy вместо AbortPolicy
        );
        threadPoolExecutor.allowCoreThreadTimeOut(true);
        return threadPoolExecutor;
    }
}