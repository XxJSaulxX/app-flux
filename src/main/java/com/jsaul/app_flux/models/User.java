package com.jsaul.app_flux.models;

public class User {

    private String name;
    private String lastName;

    /**
     * Constructors
     */

    public User() {

    }

    public User(String name, String lastName) {
        this.name = name;
        this.lastName = lastName;
    }

    /**
     * Getters and Setters
     */
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    /**
     * toString
     */

    @Override
    public String toString() {
        return "User [name=" + name + ", lastName=" + lastName + "]";
    }
    
}
