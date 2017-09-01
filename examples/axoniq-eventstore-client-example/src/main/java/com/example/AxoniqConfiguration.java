package com.example;

import io.axoniq.eventstore.client.axon.AxonIQEventStore;
import io.axoniq.eventstore.client.EventStoreConfiguration;
import com.example.command.BankAccountAggregate;
import org.axonframework.commandhandling.model.Repository;
import org.axonframework.common.caching.Cache;
import org.axonframework.common.caching.WeakReferenceCache;
import org.axonframework.common.transaction.TransactionManager;
import org.axonframework.config.EventHandlingConfiguration;
import org.axonframework.eventsourcing.*;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.messaging.annotation.ParameterResolverFactory;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.json.JacksonSerializer;
import org.axonframework.spring.eventsourcing.SpringAggregateSnapshotter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * @author Zoltan Altfatter
 */
@Configuration
public class AxoniqConfiguration {

    @Autowired
    public void config(EventHandlingConfiguration eventHandlingConfiguration) {
        eventHandlingConfiguration.registerTrackingProcessor("MyCounters");
    }

    @Bean(name = "eventBus")
    public EventStore eventStore(EventStoreConfiguration eventStoreConfiguration, Serializer serializer) {
        return new AxonIQEventStore(eventStoreConfiguration, serializer);
    }

    @Bean
    public EventStoreConfiguration eventStoreConfiguration() {
        return new EventStoreConfiguration();
    }

    @Bean
    public SpringAggregateSnapshotter snapshotter(ParameterResolverFactory parameterResolverFactory, EventStore eventStore, TransactionManager transactionManager) {
        Executor executor = Executors.newSingleThreadExecutor(); //Or any other executor of course
        return new SpringAggregateSnapshotter(eventStore, parameterResolverFactory, executor, transactionManager);
    }

    @Bean
    public SnapshotTriggerDefinition snapshotTriggerDefinition(Snapshotter snapshotter) throws Exception {
        return new EventCountSnapshotTriggerDefinition(snapshotter, 3);
    }

    @Bean
    public Cache cache(){
        return new WeakReferenceCache();
    }

    @Bean
    public Repository<BankAccountAggregate> bankAccountAggregateRepository(EventStore eventStore, SnapshotTriggerDefinition snapshotTriggerDefinition, Cache cache) {
        return new CachingEventSourcingRepository<>(new GenericAggregateFactory<>(BankAccountAggregate.class), eventStore, cache, snapshotTriggerDefinition);
    }

    @Bean
    public Serializer serializer() {
        return new JacksonSerializer();
    }
}
