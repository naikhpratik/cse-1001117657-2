/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.mavenproject4;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Scanner;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.*;
import org.apache.commons.dbcp.*;
import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import redis.clients.jedis.Jedis;

/**
 *
 * @author SuperUser
 */
public class web {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args)
    {
        System.out.println("Enter your choice do you want to work with \n 1.PostGreSQL \n 2.Redis \n 3.Mongodb");
        InputStreamReader IORdatabase=new InputStreamReader(System.in);
        BufferedReader BIOdatabase=new BufferedReader(IORdatabase);
        String Str1=null;
        try {
        Str1= BIOdatabase.readLine();
        }
        catch(Exception E7)
        {
            System.out.println(E7.getMessage());
        }
        
       // loading data from the CSV file 
        
        CsvReader data=null;
                        
                        try
                        {
                            data = new CsvReader("\\data\\Consumer_Complaints.csv");
                        }
                        catch(FileNotFoundException EB)
                        {
                            System.out.println(EB.getMessage());
                        }
                        int noofcolumn=5;
                        int noofrows =100;
                        int loops=0;
                        
                        try 
                        {
                         data = new CsvReader("\\data\\Consumer_Complaints.csv");
                        }
                        catch(FileNotFoundException E)
                        {
                                System.out.println(E.getMessage());
                        }
                     
                        String[][] Dataarray= new String[noofrows][noofcolumn];
                        try
                        {
                            while (data.readRecord())
                            {    
                            String v;
                            String[] x;
                            v = data.getRawRecord();
                            x =v.split(",");
                               for(int j=0;j<noofcolumn;j++)
                                {
                                    String value=null;
                                    int z=j;
                                    value=x[z];
                                    try
                                    {
                                    Dataarray[loops][j]=value;
                                    }
                                    catch(Exception E)
                                    {
                                        System.out.println(E.getMessage());
                                    }
                                   // System.out.println(Dataarray[iloop][j]);
                                    }
                            loops=loops+1;    
                            }
                            }
                        catch(IOException Em)
                        {
                        System.out.println(Em.getMessage());
                        }
                        
	data.close();
        
        
        // connection to Database 
       switch(Str1)
               {
                   // postgre code goes here 
                   case "1":
                       
        Connection Conn=null;
        Statement Stmt=null;
        URI dbUri =null;
        String choice=null;
        InputStreamReader objin=new InputStreamReader(System.in);
        BufferedReader objbuf=new BufferedReader(objin);
         try 
        {
         Class.forName("org.postgresql.Driver");    
        }
         catch(Exception E1)
        {
         System.out.println(E1.getMessage());
        }
        String username = "ahkyjdavhedkzg";
        String password = "2f7c3_MBJbIy1uJsFyn7zebkhY";
        String dbUrl = "jdbc:postgresql://ec2-54-83-199-54.compute-1.amazonaws.com:5432/d2l6hq915lp9vi?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory";
        
        
        
        
         // now update data in the postgress Database 
        
     /*  for(int i=0;i<RowCount;i++)
        {
             try 
                {
               
                Conn= DriverManager.getConnection(dbUrl, username, password);    
                Stmt = Conn.createStatement();
               String Connection_String = "insert into Webdata (Id,Product,state,Submitted,Complaintid) values (' "+ Dataarray[i][0]+" ',' "+ Dataarray[i][1]+" ',' "+ Dataarray[i][2]+" ',' "+ Dataarray[i][3]+" ',' "+ Dataarray[i][4]+" ')";
                 Stmt.executeUpdate(Connection_String);
               Conn.close();
                }
                catch(SQLException E4)
               {
                   System.out.println(E4.getMessage());
                }
        }
        */
        // Quering with the Database 
        
        System.out.println("1. Display Data ");
        System.out.println("2. Select data based on primary key");
        System.out.println("3. Select data based on Other attributes e.g Name");
        System.out.println("Enter your Choice ");
        try {
        choice=objbuf.readLine();
        }
        catch(IOException E)
        {
            System.out.println(E.getMessage());
        }
        switch(choice)
        {
            case "1": 
                 try 
                 {
                 Conn= DriverManager.getConnection(dbUrl, username, password);    
                 Stmt = Conn.createStatement();
                 String Connection_String = " Select * from Webdata;";
                 ResultSet objRS = Stmt.executeQuery(Connection_String);
                 while(objRS.next())
                 {
                 System.out.println(" ID: " + objRS.getInt("ID"));
                 System.out.println("Product Name: " + objRS.getString("Product"));
                 System.out.println("Which state: " + objRS.getString("State"));
                 System.out.println("Sumbitted through: " + objRS.getString("Submitted"));
                 System.out.println("Complain Number: " + objRS.getString("Complaintid"));
                    Conn.close();
                 }
                 
                 }
                 catch(Exception E2)
                 {
                 System.out.println(E2.getMessage());
                 }
                 
            break;
                
            case "2": 
                
                System.out.println("Enter Id(Primary Key) :");
                InputStreamReader objkey=new InputStreamReader(System.in);
                BufferedReader objbufkey=new BufferedReader(objkey);
                int key=0;
                try
                {
                key=Integer.parseInt(objbufkey.readLine());
                }
                catch(IOException E)
                {
                    System.out.println(E.getMessage());
                }
            try 
                 {
                 Conn= DriverManager.getConnection(dbUrl, username, password);    
                     Stmt = Conn.createStatement();
                 String Connection_String = " Select * from Webdata where Id=" + key + ";";
                 ResultSet objRS = Stmt.executeQuery(Connection_String);
                 while(objRS.next())
                 {
                 //System.out.println(" ID: " + objRS.getInt("ID"));
                 System.out.println("Product Name: " + objRS.getString("Product"));
                 System.out.println("Which state: " + objRS.getString("State"));
                 System.out.println("Sumbitted through: " + objRS.getString("Submitted"));
                 System.out.println("Complain Number: " + objRS.getString("Complaintid"));
Conn.close();
                 }
                
                 }
                 catch(Exception E2)
                 {
                 System.out.println(E2.getMessage());
                 }
            break;
                
            case "3":
                //String Name=null;
                System.out.println("Enter Complaintid to find the record");
                
               // Scanner input = new Scanner(System.in);
                //
                 int Number=0;
                try{
                    InputStreamReader objname=new InputStreamReader(System.in);
                BufferedReader objbufname=new BufferedReader(objname);
               
            Number = Integer.parseInt(objbufname.readLine());
                }
                catch(Exception E10)
                 {
                 System.out.println(E10.getMessage());
                 }
                 //System.out.println(Name);
            try 
                 {
                 Conn= DriverManager.getConnection(dbUrl, username, password);    
                 Stmt = Conn.createStatement();
                 String Connection_String = " Select * from Webdata where complaintid="  + Number+  ";";
           //2
                 System.out.println(Connection_String);
                 ResultSet objRS = Stmt.executeQuery(Connection_String);
                 while(objRS.next())
                 {
                     int id = objRS.getInt("id");
                 System.out.println(" ID: " + id);
                    System.out.println("Product Name: " + objRS.getString("Product"));
                 String state =  objRS.getString("state");
                 System.out.println("Which state: " + state);
                 String Submitted =  objRS.getString("submitted");
                 System.out.println("Sumbitted through: " + Submitted);
                 String Complaintid =  objRS.getString("complaintid");
                 System.out.println("Complain Number: " + Complaintid);
   
                 }
                 Conn.close();
                 }
                 catch(Exception E2)
                 {
                 System.out.println(E2.getMessage());
                 }
                break;
        }
        try
        {
        Conn.close();
        }
       catch(SQLException E6)
       {
           System.out.println(E6.getMessage());
       }
                   break;
                       
                       // Redis code goes here 
                       
                   case "2":
        int Length=0;
        String ID=null;
        Length= 100;

       // Connection to Redis 
        Jedis jedis = new Jedis("pub-redis-13274.us-east-1-2.1.ec2.garantiadata.com", 13274);
        jedis.auth("rfJ8OLjlv9NjRfry");
        System.out.println("Connected to Redis Database");
        
        // Storing values in the database 

       
        /* 
        for(int i=0;i<Length;i++)
        { 
         //Store data in redis 
            int j=i+1;
        jedis.hset("Record:" + j , "Product", Dataarray[i][1]);
        jedis.hset("Record:" + j , "State ", Dataarray[i][2]);
        jedis.hset("Record:" + j , "Submitted", Dataarray[i][3]);
        jedis.hset("Record:" + j , "Complaintid", Dataarray[i][4]);

        }
      */
       System.out.println("Search by \n 11.Get records through primary key \n 22.Get Records through Complaintid \n 33.Display ");
                InputStreamReader objkey=new InputStreamReader(System.in);
                BufferedReader objbufkey=new BufferedReader(objkey);
                String str2=null;
                try
                {
                str2=objbufkey.readLine();
                }
                catch(IOException E)
                {
                    System.out.println(E.getMessage());
                }
                switch(str2)
                {
                    case "11":
                        System.out.print("Enter Primary Key : ");
        InputStreamReader IORKey=new InputStreamReader(System.in);
        BufferedReader BIORKey=new BufferedReader(IORKey);
        String ID1=null;
          try 
            {
             ID1=BIORKey.readLine();
            }
            catch(IOException E3)
            {
            System.out.println(E3.getMessage());
            }
          
        Map<String, String> properties  = jedis.hgetAll("Record:" + Integer.parseInt(ID1));
        for (Map.Entry<String, String> entry : properties.entrySet())
        {
            System.out.println("Product:"+jedis.hget("Record:" + Integer.parseInt(ID1),"Product"));
            System.out.println("State:"+jedis.hget("Record:" + Integer.parseInt(ID1),"State"));
            System.out.println("Submitted:"+jedis.hget("Record:" + Integer.parseInt(ID1),"Submitted"));
            System.out.println("Complaintid:"+jedis.hget("Record:" + Integer.parseInt(ID1),"Complaintid"));
        }
                        break;
                        
                    case "22":
                        System.out.print(" Enter Complaintid  : ");
                         InputStreamReader IORName1=new InputStreamReader(System.in);
        BufferedReader BIORName1=new BufferedReader(IORName1);
          
        String ID2=null;
                try 
            {
             ID2=BIORName1.readLine();
            }
            catch(IOException E3)
            {
            System.out.println(E3.getMessage());
            }
                        for(int i=0;i<100;i++)
                        {
                             Map<String, String> properties3  = jedis.hgetAll("Record:" + i);
        for (Map.Entry<String, String> entry : properties3.entrySet())
        {
            String value=entry.getValue();
        if(entry.getValue().equals(ID2))
        {
           System.out.println("Product:"+jedis.hget("Record:" + Integer.parseInt(ID2),"Product"));
            System.out.println("State:"+jedis.hget("Record:" + Integer.parseInt(ID2),"State"));
            System.out.println("Submitted:"+jedis.hget("Record:" + Integer.parseInt(ID2),"Submitted"));
            System.out.println("Complaintid:"+jedis.hget("Record:" + Integer.parseInt(ID2),"Complaintid"));
        }
       
        }
                        }
       
          
        
        
                        break;
                        
                    case "33":
                          for(int i=1;i<21;i++)
                        {
                             Map<String, String> properties3  = jedis.hgetAll("Record:" + i);
        for (Map.Entry<String, String> entry : properties3.entrySet())
        {
           
         
         System.out.println("Product:"+jedis.hget("Record:" + i,"Product"));
            System.out.println("State:"+jedis.hget("Record:" + i,"State"));
            System.out.println("Submitted:"+jedis.hget("Record:" + i,"Submitted"));
            System.out.println("Complaintid:"+jedis.hget("Record:" + i,"Complaintid"));
            
        }
                        }
                        break;
                }
        
                   break;
                       
           
                       // mongo db 
                       
             case "3":
                        MongoClient mongo = new MongoClient(new MongoClientURI("mongodb://naikhpratik:naikhpratik@ds053964.mongolab.com:53964/heroku_6t7n587f"));
                         DB db;
                         db = mongo.getDB("heroku_6t7n587f");
                        // storing values in the database 
                        /*for(int i=0;i<100;i++)
                         {
                             BasicDBObject document = new BasicDBObject();
                            document.put("Id", i+1);
                            document.put("Product", Dataarray[i][1]);
                            document.put("State", Dataarray[i][2]);    
                            document.put("Submitted", Dataarray[i][3]);    
                            document.put("Complaintid", Dataarray[i][4]); 
                            db.getCollection("Naiknaik").insert(document);
                          
                         }*/
                System.out.println("Search by \n 1.Enter Primary key \n 2.Enter Complaintid \n 3.Display");
                InputStreamReader objkey6=new InputStreamReader(System.in);
                BufferedReader objbufkey6=new BufferedReader(objkey6);
                String str3=null;
                try
                {
                str3=objbufkey6.readLine();
                }
                catch(IOException E)
                {
                    System.out.println(E.getMessage());
                }
                switch(str3)
                {
                    case "1":
                System.out.println("Enter the Primary Key");
                InputStreamReader IORPkey=new InputStreamReader(System.in);
                BufferedReader BIORPkey=new BufferedReader(IORPkey);
                int Pkey=0;
                try
                {
                Pkey=Integer.parseInt(BIORPkey.readLine());
                }
                catch(IOException E)
                {
                    System.out.println(E.getMessage());
                }
                BasicDBObject inQuery = new BasicDBObject();
                    inQuery.put("Id", Pkey);
                         DBCursor cursor = db.getCollection("Naiknaik").find(inQuery);
                            while(cursor.hasNext()) {
                                   // System.out.println(cursor.next());
                                    System.out.println(cursor.next());
                            }
                        break;
                    case "2":
                        System.out.println("Enter the Product to Search");
                InputStreamReader objName=new InputStreamReader(System.in);
                BufferedReader objbufName=new BufferedReader(objName);
                String Name=null;
                try
                {
                Name=objbufName.readLine();
                }
                catch(IOException E)
                {
                    System.out.println(E.getMessage());
                }
                BasicDBObject inQuery1 = new BasicDBObject();
                    inQuery1.put("Product", Name);
                         DBCursor cursor1 = db.getCollection("Naiknaik").find(inQuery1);
                            while(cursor1.hasNext()) {
                                   // System.out.println(cursor.next());
                                    System.out.println(cursor1.next());
                            }
                        break;
                        
                        case "3":
                            for(int i=1;i<21;i++)
                                    {
                       BasicDBObject inQuerya = new BasicDBObject();
                    inQuerya.put("_id", i);
                         DBCursor cursora = db.getCollection("Naiknaik").find(inQuerya);
                            while(cursora.hasNext()) {
                                   // System.out.println(cursor.next());
                                    System.out.println(cursora.next());
                            }
                            }
                       break;
                }
                   break;
                       
                   
               }
        
       
       
    }
    
}

    