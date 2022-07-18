package com.example;


import java.io.IOException;

/**
 * Hello world!
 *
 */
public class App
{
    public static void main( String[] args ) throws IOException {

        GlueCatalogTesting glueCatalogTesting = new GlueCatalogTesting();
        glueCatalogTesting.createTableAndAppend();

       
}
}
