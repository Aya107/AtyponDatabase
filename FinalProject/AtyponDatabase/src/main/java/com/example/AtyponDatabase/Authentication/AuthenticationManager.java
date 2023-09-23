package com.example.AtyponDatabase.Authentication;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class AuthenticationManager {
    private static  AuthenticationManager instance;
    List<User> users = new ArrayList<>();


    public static AuthenticationManager getInstance() {
        if (instance == null) {
            instance = new AuthenticationManager();
        }
        return instance;
    }
}
