package com.ubosque.bd2_taller_4;

import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.RowMutation;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Weather extends Thread{

    private static final String PROJECT_ID = "unbosque-webprogramming";
    private static final String INSTANCE_ID = "table-1";

    private static final String COLUMN_FAMILY = "col_fam_dpradoc";
    private static final String ROW_KEY = "RowKey_";
    private static final String DATE_TIME = "DATE_TIME";
    private static final String PLANT_ID = "PLANT_ID";
    private static final String SOURCE_KEY = "SOURCE_KEY";
    private static final String AMBIENT_TEMPERATURE = "AMBIENT_TEMPERATURE";
    private static final String MODULE_TEMPERATURE = "MODULE_TEMPERATURE";
    private static final String IRRADIATION = "IRRADIATION";

    private final BigtableDataClient dataClient;
    private final BigtableTableAdminClient adminClient;
    private String nombreTabla;
    private String nombreArchivo;

    public Weather(String nombreTabla, String nombreArchivo) throws IOException {
        this.nombreTabla = nombreTabla;
        this.nombreArchivo = nombreArchivo;

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
        if(!adminClient.exists(this.nombreTabla)){
            CreateTableRequest createTableRequest = CreateTableRequest.of(nombreTabla).addFamily(COLUMN_FAMILY);
            adminClient.createTable(createTableRequest);
            System.out.printf("Tabla %s creada exitosamente.", nombreTabla);
        }else{
//            adminClient.deleteTable(TABLE_ID);
//            System.err.println("Tabla eliminada: " + TABLE_ID);
            System.err.println("Tabla ya existe");
        }
    }

    public void cargarClima(){
        try (BufferedReader bufferedReader = new BufferedReader(
                new FileReader(this.nombreArchivo)
        )) {
            String line = bufferedReader.readLine();
            int i = 1;
            while (line != null) {
                String[] data = line.split(",");
                if (!data[0].contains(DATE_TIME)) {
                    RowMutation rowMutation = RowMutation.create(this.nombreTabla, ROW_KEY + i)
                            .setCell(COLUMN_FAMILY, DATE_TIME, data[0])
                            .setCell(COLUMN_FAMILY, PLANT_ID, data[1])
                            .setCell(COLUMN_FAMILY, SOURCE_KEY, data[2])
                            .setCell(COLUMN_FAMILY, AMBIENT_TEMPERATURE, data[3])
                            .setCell(COLUMN_FAMILY, MODULE_TEMPERATURE, data[4])
                            .setCell(COLUMN_FAMILY, IRRADIATION, data[5]);
                    dataClient.mutateRow(rowMutation);
                    System.out.println("Creacion registro Wheather: " + ROW_KEY + i);
                }

                line = bufferedReader.readLine();
                i++;
            }
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }

    public void obtenerClave(){
        try {
            Query query = Query.create(this.nombreTabla);
            ServerStream<Row> rowStream = dataClient.readRows(query);
            List<Row> rowList = new ArrayList<>();
            for (Row row : rowStream){
                rowList.add(row);
            }
            List<RowCell> rowCellList = new ArrayList<>();
            for (int i = 0; i < rowList.size(); i++) {
                for(RowCell rowCell : rowList.get(i).getCells()){
                    rowCellList.add(rowCell);
                }
            }

            Object[] resultado = rowCellList.stream().filter(
                    rc -> rc.getQualifier().toStringUtf8().contains(SOURCE_KEY) &&
                            rc.getValue().toStringUtf8().contains("adLQvlD726eNBSB")
            ).toArray();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void run(){
        cargarClima();
    }
}
