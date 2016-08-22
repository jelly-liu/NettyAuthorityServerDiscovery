package com.jelly.example;

/**
 * Created by jelly on 2016-8-22.
 */

/**
 * In a real application, the Service payload will most likely
 * be more detailed than this. But, this gives a good example.
 */
public class InstanceDetailsExample {
    private String description;

    public InstanceDetailsExample() {
        this("");
    }

    public InstanceDetailsExample(String description) {
        this.description = description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getDescription() {
        return description;
    }

    @Override
    public String toString() {
        return this.description;
    }
}
