package com.mwclg.bean;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class DataLine {
    private String merge_type;
    private String db;
    private String table;
    private String data;
}
