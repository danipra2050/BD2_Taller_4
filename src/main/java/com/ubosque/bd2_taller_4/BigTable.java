package com.ubosque.bd2_taller_4;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class BigTable {

    private static final String PROJECT_ID = "unbosque-webprogramming";
    private static final String INSTANCE_ID = "table-1";
    private static final String TABLE_ID = "Planta_1_dpradoc";

    private static final String COLUMN_FAMILY = "col_fam_dpradoc";

    private BigtableDataClient dataClient;
    private BigtableTableAdminClient adminClient;

    public BigTable() throws IOException {
        BigtableDataSettings settings = BigtableDataSettings.newBuilder()
                .setProjectId(PROJECT_ID)
                .setInstanceId(INSTANCE_ID)
                .build();

        BigtableTableAdminSettings adminSettings = BigtableTableAdminSettings.newBuilder()
                .setProjectId(PROJECT_ID)
                .setInstanceId(INSTANCE_ID)
                .build();

        dataClient = BigtableDataClient.create(settings);
        adminClient = BigtableTableAdminClient.create(adminSettings);
    }

    public void crearTabla(){
        if(!adminClient.exists(TABLE_ID)){
            CreateTableRequest createTableRequest = CreateTableRequest.of(TABLE_ID).addFamily(COLUMN_FAMILY);
            adminClient.createTable(createTableRequest);
            System.out.printf("Tabla %s creada exitosamente.", TABLE_ID);
        }else{
            System.err.println("La tabla ya existe: " + TABLE_ID);
        }
    }

    public void cargarDatosPlanta1() throws IOException {
        BufferedReader bufferedReader = new BufferedReader(
                new FileReader("./target/files/Plant_1_Generation_Data.csv")
        );
        String line = bufferedReader.readLine();
        while(line != null){
            String[] data = line.split(",");
            
        }
    }
}
