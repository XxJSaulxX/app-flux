package com.jsaul.app_flux.models;

public class UserComments {

    private User user;
    private Comments comments;

    /**
     * Constructors
     */

    public UserComments(User user, Comments comments) {
        this.user = user;
        this.comments = comments;
    }

    /**
     * ToString method
     */

    @Override
    public String toString() {
        return "UserComments [user=" + user + ", comments=" + comments + "]";
    }

}
