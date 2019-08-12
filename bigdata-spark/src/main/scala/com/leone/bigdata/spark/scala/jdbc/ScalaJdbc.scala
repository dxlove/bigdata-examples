package com.leone.bigdata.spark.scala.jdbc

import scalikejdbc.config.DBs
import scalikejdbc.{DB, SQL}

/**
  * <p>
  *
  * @author leone
  * @since 2019-03-09
  **/
object ScalaJdbc {

  def main(args: Array[String]): Unit = {
    // 加载配置
    DBs.setup()

    //insert(User(1, "test", new Date(), false, "100231", 18, "good man"))

    //println(select(1470819L))

    //update(User(1470819L, "testUpdate", new Date(), deleted = true, "10000", 28, "good person"))

    delete(1470819L)

    //for (user <- select(10)) {
    //  println(user)
    //}

  }

  /**
    * 读取使用的是readOnly
    *
    * @param limit
    * @return
    */
  def select(limit: Int): List[User] = {
    DB.readOnly(implicit session => {
      SQL("select * from t_user limit ?").bind(limit).map(rs => {
        User(rs.long("user_id"), rs.string("account"), rs.date("create_time"), rs.boolean("deleted"), rs.string("password"), rs.int("age"), rs.string("description"))
      }).list().apply()
    })
  }

  /**
    * 读取使用的是readOnly
    *
    * @param userId
    * @return
    */
  def select(userId: Long): User = {
    DB.readOnly(implicit session => {
      SQL("select * from t_user where user_id = ?").bind(userId).map(rs => {
        User(rs.long("user_id"), rs.string("account"), rs.date("create_time"), rs.boolean("deleted"), rs.string("password"), rs.int("age"), rs.string("description"))
      }).toOption().apply().get
    })
  }

  /**
    * 插入使用的是localTx
    *
    * @param user
    */
  def insert(user: User): Unit = {
    DB.localTx(implicit session => {
      SQL("insert into t_user(account,create_time,deleted,password,age,description) values (?, ?, ?, ?, ?, ?)")
        .bind(user.account, user.createTime, user.deleted, user.password, user.age, user.description).update().apply()
    })
  }

  /**
    * 修改
    *
    * @param user
    */
  def update(user: User): Unit = {
    DB.autoCommit(implicit session => {
      // SQL里面是普通的sql语句，后面bind里面是语句中"?"的值，update().apply()是执行语句
      SQL("update t_user set account = ?, password = ?, age = ?, description = ?, deleted = ? where user_id = ?")
        .bind(user.account, user.password, user.age, user.description, user.deleted, user.userId)
        .update()
        .apply()
    })

  }

  /**
    * 删除 autoCommit
    *
    * @param id
    */
  def delete(id: Long): Unit = {
    DB.autoCommit(implicit session => {
      SQL("delete from t_user where user_id = ?").bind(id).update().apply()
    })
  }

  case class User(userId: Long, account: String, createTime: java.util.Date, deleted: Boolean, password: String, age: Int, description: String) {}

}

