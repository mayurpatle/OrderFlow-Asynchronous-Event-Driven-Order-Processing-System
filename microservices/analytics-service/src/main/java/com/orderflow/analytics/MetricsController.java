package com.orderflow.analytics;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * Serves the aggregated metrics over HTTP.
 *
 * GET /metrics returns a JSON snapshot of all the counters analytics has
 * accumulated. In production this might be Prometheus-format text for scraping,
 * or it might push to a dashboarding tool. For our purposes, plain JSON is
 * perfect — you can hit it in a browser or curl and watch the numbers climb.
 */
@RestController
@RequestMapping("/metrics")
@RequiredArgsConstructor
public class MetricsController {

    private final AnalyticsEventConsumer consumer;

    @GetMapping
    public Map<String, Object> metrics() {
        return consumer.snapshot();
    }
}