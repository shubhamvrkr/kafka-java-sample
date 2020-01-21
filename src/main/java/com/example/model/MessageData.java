package com.example.model;

import lombok.*;

import java.util.List;

@ToString
@NoArgsConstructor
@AllArgsConstructor
@Setter
@Getter
/**
 * Model object to publish in kafka
 */
public class MessageData {

    private String id;
    private String type;
    private List<String> data;
}
