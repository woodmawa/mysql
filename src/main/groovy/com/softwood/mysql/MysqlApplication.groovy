package com.softwood.mysql

import org.springframework.boot.CommandLineRunner
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import com.mysql.cj.xdevapi.*

import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit

@SpringBootApplication
class MysqlApplication implements CommandLineRunner {

    static void main(String[] args) {
        SpringApplication.run(MysqlApplication, args)
    }

    @Override
    public void run(String... args) {

        //add session code here

        // Connect to server on localhost - x api listens by default on 33060
        Session mySession
        mySession = new SessionFactory().getSession("mysqlx://localhost:33060/test?user=will&password=pwd")
        /*mySession = new SessionFactory().getSession (
                [name:"will", password:"pwd",
                host: "localhost",
                port: 33060] as Properties
        )*/

        Schema myDb = mySession.getSchema("test")

        // Use the collection 'my_collection'

        myDb.dropCollection("people")
        Collection people = myDb.createCollection("people")
        Collection myColl = myDb.getCollection("people")

        // Insert documents
        myColl.add(/{"name":"Will", "age":57, "spouse":"marian" }/).execute()
        myColl.add("{\"name\":\"Sakila\", \"age\":15}").execute()
        myColl.add("{\"name\":\"Susanne\", \"age\":24}").execute()
        myColl.add("{\"name\":\"User\", \"age\":39}").execute()


        // Find a document
        DocResult docs = myColl.find("name like :name AND age < :age")
                .bind("name", "S%").bind("age", 20).execute()

        assert docs.count() == 1
        /* Specify which document to find with Collection.find() and
         fetch it from the database with .execute()
        */
        DocResult myDocs = myColl.find("name like :param").limit(10).bind("param", "S%").execute();

        assert myDocs.count() == 2

        // Print document
        println(myDocs.fetchOne())

        println "multi row insert - insert bulk records "

        def v
        def org
         def start = System.nanoTime()
        def vArr = []
        List aRes = []
        //write 100k records
        for (int i=1; i<1001;i++) {
            vArr <<  /{"name":"person#[$i]", "age":$i, "spouse":"marian", "inaugurated":2000 }/

        }
        println vArr.size() + " person  to insert in mysql document"

        vArr.each {aRes << myColl.add(it).execute()}

        def end = System.nanoTime()
        def duration = (end - start)
        def period = TimeUnit.MILLISECONDS.convert(duration, TimeUnit.NANOSECONDS)/1000

        println "mysql 1000 records async done in duration " + period + " seconds"

        aRes*.join()
        mySession.close()
    }
}
