import java.sql.DriverManager
import java.util.{Date, Properties}

import scala.io.Source
import com.sun.org.apache.xpath.internal.operations.And
import org.jsoup.Jsoup

import scala.collection.JavaConversions._
import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.collection.mutable._

/**
  *
  * Created by Siddharth on 9/6/2016.
  */

/** Categorize the uncategorized expenses */
object classifyIncome extends App {

  val JDBC_DRIVER = "org.postgresql.Driver"
  val DB_URL = "jdbc:postgresql://ec2-54-221-225-242.compute-1.amazonaws.com:5432/dr6m17bo34l45?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory"
  val PASS = "IAolGhH1x_71ZhX8JMqbDXcj_k"
  var merchantMap = HashMap(("", ""))

  //STEP 2: Register JDBC driver
  Class.forName(JDBC_DRIVER)
  val props = new Properties()
  props.setProperty("user", "xdqwfnfvmonsep")
  props.setProperty("password", "IAolGhH1x_71ZhX8JMqbDXcj_k")
  props.setProperty("ssl", "true")
  //STEP 3: Open a connection
  System.out.println("Connecting to database...")
  val conn = DriverManager.getConnection(DB_URL, props)
  val statement = conn.createStatement()
  //
  //Important-> following statement to be executed only once-------------------------------------------------------------at the start
  statement.executeUpdate("UPDATE account_transaction\n Set Use=-1 ;")
  val sqlString = "Select  DISTINCT user_id from account_transaction ;"
  var rs = statement.executeQuery(sqlString)
  var _User = Seq("")
  while (rs.next()) {
    _User= _User:+rs.getString("user_id")
  }
  for(user <- _User.slice(1,_User.length)) {
    println("\n---------------------------------------------------- \n")
    println(user)


    statement.executeUpdate(s"""UPDATE account_transaction SET Use=0 FROM ( SELECT _id FROM account_transaction WHERE user_id='$user' and transaction_type= 'ACCOUNT_DEBIT' and Use=-1 ORDER by transaction_time ) AS dst WHERE account_transaction._id=dst._id ;""")
    statement.executeUpdate(s"""UPDATE account_transaction SET Use=0 WHERE _id = (SELECT _id FROM account_transaction WHERE user_id='$user' and transaction_type= 'ACCOUNT_CREDIT' ORDER by transaction_time LIMIT 1);""")

    statement.executeUpdate(s"UPDATE account_transaction SET Use=0 WHERE _id in (SELECT _id FROM account_transaction WHERE user_id='$user' and transaction_type='ACCOUNT_CREDIT'\nORDER BY transaction_time ASC\nLIMIT 1);")
    var al = statement.executeQuery(s"""SELECT _id from account_transaction WHERE user_id='$user' and transaction_type='ACCOUNT_CREDIT' and Use=-1 ORDER BY transaction_time ASC;""")
    var id1 = Seq(0)
    while (al.next()) {
      id1 = id1 :+ al.getInt("_id")
    }
    for (id <- id1.slice(1, id1.length)) {
      //println(id)
      var m1 = statement.executeQuery(s"SELECT * from account_transaction WHERE _id=$id ;")
      if (m1.next()) {

        var m2 = m1.getDouble("amount")
        var m3 = 1.05 * m2
        m2 = .95 * m2
        //println(s"amount->$m2")
        var m4 = m1.getString("transaction_time")
        var a2 = statement.executeQuery(s"""SELECT _id from account_transaction WHERE user_id='$user' and transaction_type='ACCOUNT_DEBIT' and Use=0 and amount between $m2 and $m3 and age(transaction_time,'$m4') < interval '5 days' ORDER BY transaction_time DESC LIMIT 1 ;""")
        //var m5 = a2.getInt("_id")
        if (a2.next()) {
          var m5 = a2.getInt("_id")
          statement.executeUpdate(s"UPDATE account_transaction SET Use=$id WHERE _id=$m5;")
          statement.executeUpdate(s"UPDATE account_transaction SET Use=$m5 WHERE _id=$id;")
        }
        else
          {
            statement.executeUpdate(s"UPDATE account_transaction SET Use=0 WHERE _id=$id;")
          }
      }
    }



      var al1=statement.executeQuery(s"""SELECT _id from account_transaction WHERE user_id='$user' and transaction_type='ACCOUNT_CREDIT' and Use=0 ORDER BY transaction_time ASC;""")
      var id11=Seq(0)
      while (al1.next()) {
        id11 = id11 :+ al1.getInt("_id")
      }
        for(id <- id11.slice(1,id11.length)) {
          var m11 = statement.executeQuery(s"SELECT amount,transaction_time from account_transaction WHERE _id=$id ;")
          if (m11.next()) {
            var m44 = m11.getDate("transaction_time")
            var m22 = .95 * m11.getDouble("amount")
            var m33 = 1.05 * m11.getDouble("amount")
            var a22 = statement.executeQuery(s"""SELECT _id from account_transaction WHERE user_id='$user' and transaction_type='ACCOUNT_CREDIT' and Use!=0 and amount between $m22 and $m33 and age(transaction_time,'$m44') between interval '26 days' and interval '34 days'  \nORDER BY transaction_time DESC LIMIT 1 ;""")
            //var m55= a22.getInt("_id")
            if (a22.next()) {
              var m55 = a22.getInt("_id")
              statement.executeUpdate(s"UPDATE account_transaction SET Use=0 WHERE _id in (SELECT Use from account_transaction WHERE _id=$m55);")
              statement.executeUpdate(s"UPDATE account_transaction SET Use=0 WHERE _id=$m55;")
            }
          }
        }


      println("TRANSACTIONS---------> ")
      var m111=statement.executeQuery(s"""SELECT _id,amount,transaction_time,transaction_type,transaction_mode,Use from account_transaction WHERE user_id='$user' ORDER by transaction_time ASC;""")
      var id111=Seq((0,0.0,new Date(),"","",0))
      while (m111.next()) {

        id111 = id111:+(m111.getInt("_id"),m111.getDouble("amount"), m111.getDate("transaction_time"), m111.getString("transaction_mode"), m111.getString("transaction_type"),m111.getInt("Use"))

      }//print("sadasdddsad")
      for(id <- id111.slice(1,id111.length)) {
        println("id->  "+ id._1 + " Amount->  "+ id._2 + " Date->  "+ id._3 + " Mode->  "+ id._4 + "  Type->  "+ id._5 + "  Use->  " + id._6)

        /*var t1=id._5
        var t2=id._1
        //println(t2,"  ",t1)
        var _m111=statement.executeQuery(s"SELECT _id,amount,transaction_time,transaction_mode,Use from account_transaction WHERE _id=$t1;")
        while(_m111.next()){
          //println("debit walee----->",_m111.getInt("_id")," ",_m111.getDouble("amount"), _m111.getDate("transaction_time"), _m111.getString("transaction_mode"),_m111.getInt("Use"))
        }
        */

      }
/*
      println("EXPENSE TRANSACTIONS---------> ")
      var m1111=statement.executeQuery(s"""SELECT amount,transaction_time,transaction_mode from account_transaction WHERE user_id='$user' and Use!=0 and transaction_type='ACCOUNT_DEBIT' ORDER by transaction_time ASC;""")
      var id1111=Seq(0.0,"","")
      while (m1111.next()) {
        id1111=id1111:+(m1111.getDouble("amount"),m1111.getDate("transaction_time"),m1111.getString("transaction_mode"))
        //println("asdsaddas")
      }
      for(id <- id1111.slice(3,id1111.length)) {
        println(id)
      }

*/
    }



  /*def CSVReader(absPath:String, delimiter:String): List[List[Any]] = {
    println("Now reading... " + absPath)
    val MasterList = Source.fromFile(absPath).getLines().toList map {
      // String#split() takes a regex, thus escaping.
      _.split("""\""" + delimiter).toList
    }
    return MasterList
  }

  var file = "D:\\ClassifyIncome\\src\\transactions.csv"
  var delimiter = ","

  var content = CSVReader(file, delimiter)
  //print(content(1)(1))

*/


}




/*
ALTER TABLE tbl
ADD  COLUMN Use int NOT NULL DEFAULT -1 ;

SELECT * from  tbl
ORDER BY date;

SELECT id, name, birthday FROM employee.person
WHERE EXTRACT(MONTH FROM birthday) = 10;

if(rs.category("type")=="Credit")

val rs = statement.executeQuery(sqlString)
var merchantList = Seq("BALAJI PURE VEG-THAKUR")
while (rs.next() & co<1) {
  merchantList= merchantList:+rs.getString("merchant")
  co+=1
}
SELECT * from tbl
WHERE Type = "Debit" and Use=0 and Amount BETWEEN 9.5*rs.category("Amount") and 10.5*rs.category("Amount")
for i in merchantList
{

}
*/