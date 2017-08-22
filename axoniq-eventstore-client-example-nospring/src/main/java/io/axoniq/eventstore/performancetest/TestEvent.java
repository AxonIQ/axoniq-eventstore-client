package io.axoniq.eventstore.performancetest;

/**
 * Author: marc
 */
public class TestEvent {
    private String name;
    private String description;

    public TestEvent(String name, String description) {
        this.name = name;
        this.description = description;
    }

    public TestEvent() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}
