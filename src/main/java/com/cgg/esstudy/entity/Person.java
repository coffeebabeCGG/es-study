package com.cgg.esstudy.entity;

import lombok.*;

import java.util.Date;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class Person {

    private Long id;

    private String name;

    private Integer age;

    private Date birthDay;


}
