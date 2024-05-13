package org.example;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class Main {
    public static void main(String[] args) {

        List<String> list = new ArrayList<String>();
        HashSet<String> listttt = new HashSet<>();
        File file = new File("C:\\Users\\Vla\\Desktop\\big\\google.txt");
        if(file.exists()){
            try {
                list = Files.readAllLines(file.toPath(), Charset.defaultCharset());
            } catch (IOException ex) {
                ex.printStackTrace();
            }
            if(list.isEmpty())
                return;
        }
        for(String line : list){
            String [] res = line.split("\t");
            for (String word : res) {
               listttt.add(word);
            }
        }
        System.out.println(listttt.size());
    }
}