package com.example.BootStrapper.Controllers;

import com.example.BootStrapper.AsyncRequests;
import com.example.BootStrapper.Authentication.User;
import com.example.BootStrapper.Authentication.UsersManager;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;


@RestController
@RequestMapping("database")
@RequiredArgsConstructor
public class BootStrappingController {
    private final AsyncRequests asyncRequests = new AsyncRequests();

    @PostMapping("")
    public void getStartupMessage(@RequestBody InitPingMessage initPingMessage) throws InterruptedException {
        asyncRequests.sync(initPingMessage.getPort());
    }

    @PostMapping("register") // Add a new endpoint for user registration
    public ResponseEntity<String> registerUser(@RequestBody User user) throws IOException {
        String userNode = UsersManager.getInstance().register(user);
        return ResponseEntity.ok("User registered successfully");
    }

    @PostMapping("login")
    public void loginUser(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        String username = request.getParameter("username");
        String password = request.getParameter("password");

        boolean isValidUser = false;
        isValidUser = UsersManager.getInstance().Authenticate(username, password);


        if (isValidUser)
        {
            User user = new User(username, password);
            request.setAttribute("user", user);
            request.getRequestDispatcher("/WEB-INF/jsp/welcome.jsp").forward(request, response);
        } else {
            request.setAttribute("errorMessage", "Invalid Credentials!!");
            request.getRequestDispatcher("/WEB-INF/jsp/login.jsp").forward(request, response);
        }
    }
}
