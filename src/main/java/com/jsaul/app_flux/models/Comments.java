package com.jsaul.app_flux.models;

import java.util.ArrayList;
import java.util.List;

public class Comments {

    private List<String> comments;

    public Comments() {
        this.comments = new ArrayList<>();
    }

    /**
     * Add a comment to the list of comments.
     * @param comment
     */
    public void addComment(String comment) {
        this.comments.add(comment);
    }

    @Override
    public String toString() {
        return "Comments = " + comments ;
    }

    
}
