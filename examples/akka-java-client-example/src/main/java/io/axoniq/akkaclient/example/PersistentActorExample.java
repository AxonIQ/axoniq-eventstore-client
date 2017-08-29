/**
 * Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
 */

package io.axoniq.akkaclient.example;

//#persistent-actor-example

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.persistence.AbstractPersistentActor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.UUID;

class Cmd implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String data;

    public Cmd(String data) {
        this.data = data;
    }

    public String getData() {
        return data;
    }
}

class Evt implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String data;

    public Evt() { data = null; }

    public Evt(String data) {
        this.data = data;
    }

    public String getData() {
        return data;
    }
}

class ExampleState implements Serializable {
    private static final long serialVersionUID = 1L;
    private final ArrayList<String> events;

    public ExampleState() {
        this(new ArrayList<>());
    }

    public ExampleState(ArrayList<String> events) {
        this.events = events;
    }

    public ExampleState copy() {
        return new ExampleState(new ArrayList<>(events));
    }

    public void update(Evt evt) {
        events.add(evt.getData());
    }

    public int size() {
        return events.size();
    }

    @Override
    public String toString() {
        return events.toString();
    }
}

class ExamplePersistentActor extends AbstractPersistentActor {

    private final String persistenceId;

    public ExamplePersistentActor(String persistenceId) {
        this.persistenceId = persistenceId;
    }

    private ExampleState state = new ExampleState();

    public int getNumEvents() {
        return state.size();
    }

    @Override
    public String persistenceId() { return persistenceId; }

    @Override
    public Receive createReceiveRecover() {
        return receiveBuilder()
            .match(Evt.class, state::update)
            .build();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(Cmd.class, (Cmd c) -> {
                persist(new Evt("received:" + c.getData()), (Evt evt) -> {
                    state.update(evt);
                    getContext().system().eventStream().publish(evt);
                });
            })
            .matchEquals("print", s -> System.out.println(state))
            .build();
    }

}
//#persistent-actor-example

public class PersistentActorExample {
    public static void main(String... args) throws Exception {
        String aggregateId = UUID.randomUUID().toString();
        String actorId = UUID.randomUUID().toString();

        final ActorSystem system = ActorSystem.create("example");
        final ActorRef persistentActor = system.actorOf(Props.create(ExamplePersistentActor.class, aggregateId), actorId);
        persistentActor.tell(new Cmd("Hello"), null);
        persistentActor.tell(new Cmd("World!"), null);
        persistentActor.tell("print", null);
        Thread.sleep(5000);
        system.terminate();

        final ActorSystem system2 = ActorSystem.create("example");
        final ActorRef persistentActor2 = system2.actorOf(Props.create(ExamplePersistentActor.class, aggregateId), actorId);
        persistentActor2.tell("print", null);
        Thread.sleep(5000);
        system2.terminate();
    }
}
