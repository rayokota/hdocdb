package io.hdocdb.store.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.ojai.types.ODate;

import java.util.ArrayList;
import java.util.List;

public class User2 {

    private String id;
    private String firstName;
    private String lastName;

    @JsonCreator
    public User2(@JsonProperty("_id") String id,
                 @JsonProperty("firstName") String firstName,
                 @JsonProperty("lastName") String lastName) {
        this.id = id;
        this.firstName = firstName;
        this.lastName = lastName;
    }

    @JsonProperty("_id")
    public String getId() {
        return id;
    }

    public String getFirstName() {
        return firstName;
    }

    public String getLastName() {
        return lastName;
    }
}
