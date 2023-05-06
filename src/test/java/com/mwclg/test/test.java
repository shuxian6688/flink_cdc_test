package com.mwclg.test;

import com.mwclg.utils.GetConfInfoUtils;

public class test {
    public static void main(String[] args) {
        System.out.println(new GetConfInfoUtils().getProperty("mysql_port"));
        test();
    }

    public static void test(){
        System.out.println(new GetConfInfoUtils().getProperty("mysql_port"));
    }
}
