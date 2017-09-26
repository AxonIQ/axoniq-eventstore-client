/*
 * Copyright (c) 2017. AxonIQ
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
