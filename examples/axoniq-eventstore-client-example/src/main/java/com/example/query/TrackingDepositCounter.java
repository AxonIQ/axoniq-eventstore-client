package com.example.query;

import com.example.events.MoneyDepositedEvent;
import org.axonframework.config.ProcessingGroup;
import org.axonframework.eventhandling.EventHandler;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by marc on 7/17/2017.
 */
@Component
@ProcessingGroup("MyCounters")
public class TrackingDepositCounter {
    private AtomicLong deposits = new AtomicLong();
    @EventHandler
    public void on(MoneyDepositedEvent event) {
        System.out.println( "# deposits: " + deposits.incrementAndGet());
    }
}
