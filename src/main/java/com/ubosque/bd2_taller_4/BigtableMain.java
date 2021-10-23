package com.ubosque.bd2_taller_4;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;

public class BigtableMain {
    private static final String TABLE_ID = "Planta_1_dpradoc";
    private static final String TABLE_W_ID = "Weather_dpradoc";

    public static void main (String[] args) throws Exception{
        BigTable bigTable = new BigTable(TABLE_ID, "./target/files/Plant_1_Generation_Data.csv");
//        bigTable.crearTabla(TABLE_ID);
//        bigTable.cargarDatosPlanta();
//        bigTable.start();
        bigTable.obtenerLista();

        BigTable plantaDos = new BigTable(TABLE_ID, "./target/files/Plant_2_Generation_Data.csv");
//        plantaDos.cargarDatosPlanta();
//        plantaDos.start();

        Weather plantaUno = new Weather(TABLE_W_ID, "./target/files/Plant_1_Weather_Sensor_Data.csv");
//        plantaUno.crearTabla();
//        plantaUno.cargarClima();
//        plantaUno.start();

        Weather plantaDosW = new Weather(TABLE_W_ID, "./target/files/Plant_2_Weather_Sensor_Data.csv");
//        plantaDosW.cargarClima();
//        plantaDosW.start();
    }
}
