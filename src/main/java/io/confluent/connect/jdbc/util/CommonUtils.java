package io.confluent.connect.jdbc.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.Inet4Address;
import java.util.ArrayList;

public class CommonUtils {

    private static Logger log = LoggerFactory.getLogger(CommonUtils.class);;

    public static void deleteFile(File file) {
        try {
            file.delete();
        } catch (Exception e) {
            log.error("Error deleting file {}", file.getAbsolutePath(), e);
        }
    }


    public static String getLocalIpOrHost() {
        String localIpOrHost = null;
        try {
            localIpOrHost = Inet4Address.getLocalHost().getHostAddress();
        } catch (Exception e) {
            log.error("Error getting local ip address", e);
        }

        if (localIpOrHost == null) {
            try {
                localIpOrHost = Inet4Address.getLocalHost().getHostName();
            } catch (Exception e) {
               log.error("Error getting local host name", e);
            }
        }
        return localIpOrHost;
    }

    public static ArrayList<String> executeCommand(String command) throws Exception{

        ArrayList<String> output = new ArrayList<String>();

        Process p;

        try {

            p = Runtime.getRuntime().exec(command);
            p.waitFor();
            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));

            String line = "";
            while ((line = reader.readLine()) != null) {
                output.add(line + "\n");
            }
        } catch (Exception e) {
            log.error("Error executing command {}", command, e);
            throw  e;
        }
        return output;
    }
}
