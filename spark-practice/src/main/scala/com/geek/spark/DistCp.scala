package com.geek.spark

import org.apache.commons.cli.{CommandLine, CommandLineParser, HelpFormatter, Option, Options, ParseException, PosixParser}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.{Accumulator, Partitioner, SparkConf, SparkContext}

import java.net.URI
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object DistCp {

  var source: String = null
  var target: String = null
  var needIgnore: Boolean = false
  var maxPartition: Int = 8

  def main(args: Array[String]): Unit = {

    // ----------------解析参数-------------------


    val options = new Options()
    val opt1 = new Option("h", "help", false, "Print help")
    opt1.setRequired(false)
    options.addOption(opt1)

    val opt2 = new Option("s", "source", true, "源目录")
    opt2.setRequired(true)
    options.addOption(opt2)

    val opt3 = new Option("t", "target", true, "目标目录")
    opt3.setRequired(true)
    options.addOption(opt3)

    val opt4 = new Option("i", "ignore", false, "忽略错误")
    opt4.setRequired(false)
    options.addOption(opt4)

    val opt5 = new Option("m", "max concurrence", true, "最大并行度")
    opt5.setRequired(false)
    options.addOption(opt5)

    val hf = new HelpFormatter()
    hf.setWidth(110)
    var commandLine: CommandLine = null
    val parser: CommandLineParser = new PosixParser()
    args.foreach(println(_))
    try {
      commandLine = parser.parse(options, args)
      if (commandLine.hasOption('h')) {
        // 打印使用帮助
        hf.printHelp("testApp", options, true)
      }

      // 打印opts的名称和值
      System.out.println("--------------------------------------")
      val opts = commandLine.getOptions
      if (opts != null) {
        opts.foreach(opt => {
          opt.getOpt match {
            case "s" => source = commandLine.getOptionValue("s")
            case "t" => target = commandLine.getOptionValue("t")
            case "i" => needIgnore = true
            case "m" => maxPartition = commandLine.getOptionValue("m").toInt
            case shortName =>
              val value: String = commandLine.getOptionValue(shortName)
              System.out.println(shortName + "=>" + value)
          }
        })
      }
    }
    catch {
      case e: ParseException => {
        hf.printHelp("testApp", options, true)
        throw e
      }
    }
    // ----------------解析参数-------------------


    val conf = new SparkConf()
    conf.setMaster("local[8]")
    conf.setAppName("distcp")
    val sc = new SparkContext(conf)
    val hadoopConf = sc.hadoopConfiguration

    accumulator = sc.accumulator(-1, "partition_key")

    // 先遍历文件再分发
    val tuples: mutable.Seq[(String, String)] = getAllFiles(source)
      .map(_.toString)
      .map(sourcePath => (sourcePath, target + sourcePath.replaceFirst(source, "")))

    sc.makeRDD(tuples)
      .partitionBy(new FairPartitioner(maxPartition))
      .foreachPartition(iterator => {
        // 每个分区同步执行,不能每条并行执行
        iterator.foreach(pair => {
          println(pair._1 + pair._2)
          //          FileUtil.copy(f)
        })
      })

    //    val data: RDD[(String, PortableDataStream)] = sc.binaryFiles(source)
    //    data.partitionBy(new FairPartitioner(4))
    //      .foreachPartition(f => {
    //        // 这里走同步流程,每个partition同步处理一条通道,不至于所有文件都开流
    //        while (f.hasNext) {
    //          val x = f.next()
    //          val path = target + x._2.getPath().substring(x._2.getPath().lastIndexOf("/"))
    //          val dir = new Path(path)
    //
    //          val fs = FileSystem.get(URI.create(path), sc.hadoopConfiguration)
    //          val inputStream = x._2.open()
    //          val outStream: FSDataOutputStream = fs.create(dir)
    //          IOUtils.copyBytes(inputStream, outStream, 4096, true)
    //          outStream.close()
    //          inputStream.close()
    //          fs.close()
    //        }
    //      })
  }

  def getHdfs(path: String) = {
    val conf = new Configuration()
    FileSystem.get(URI.create(path), conf)
  }

  def getFilesAndDirs(path: String): Array[Path] = {
    val fs = getHdfs(path).listStatus(new Path(path))
    FileUtil.stat2Paths(fs)
  }

  /** ************直接打印*********** */

  /**
   * 打印所有的文件名，包括子目录
   */
  def listAllFiles(path: String) {
    val hdfs = getHdfs(path)
    val listPath = getFilesAndDirs(path)
    listPath.foreach(path => {
      if (hdfs.getFileStatus(path).isFile())
        println(path)
      else {
        listAllFiles(path.toString())

      }
    })
  }

  /**
   * 打印一级文件名
   */
  def listFiles(path: String) {
    getFilesAndDirs(path).filter(getHdfs(path).getFileStatus(_).isFile()).foreach(println)
  }

  /**
   * 打印一级目录名
   */
  def listDirs(path: String) {
    getFilesAndDirs(path).filter(getHdfs(path).getFileStatus(_).isDirectory()).foreach(println)
  }

  /**
   * 打印一级文件名和目录名
   */
  def listFilesAndDirs(path: String) {
    getFilesAndDirs(path).foreach(println)
  }

  /** ************直接打印*********** */

  /** ************返回数组*********** */
  def getAllFiles(path: String): ArrayBuffer[Path] = {
    val arr = ArrayBuffer[Path]()
    val hdfs = getHdfs(path)
    val listPath = getFilesAndDirs(path)
    listPath.foreach(path => {
      if (hdfs.getFileStatus(path).isFile()) {
        arr += path
      } else {
        arr ++= getAllFiles(path.toString())
      }

    })
    arr
  }

  def getFiles(path: String): Array[Path] = {
    getFilesAndDirs(path).filter(getHdfs(path).getFileStatus(_).isFile())
  }

  def getDirs(path: String): Array[Path] = {
    getFilesAndDirs(path).filter(getHdfs(path).getFileStatus(_).isDirectory())
  }

  def moveFiles(fileSystem: FileSystem, mergeTime: String, fromDir: String,
                destDir: String, ifTruncDestDir: Boolean): Unit = {

    val fromDirPath = new Path(fromDir)
    val destDirPath = new Path(destDir)

    if (!fileSystem.exists(new Path(destDir))) {
      fileSystem.mkdirs(destDirPath.getParent)
    }

    // 是否清空目标目录下面的所有文件
    if (ifTruncDestDir) {
      fileSystem.globStatus(new Path(destDir + "/*")).foreach(x => fileSystem.delete(x.getPath(), true))
    }

    var num = 0
    fileSystem.globStatus(new Path(fromDir + "/*")).foreach(x => {

      val fromLocation = x.getPath().toString
      val fileName = fromLocation.substring(fromLocation.lastIndexOf("/") + 1)
      val fromPath = new Path(fromLocation)

      if (fileName != "_SUCCESS") {
        var destLocation = fromLocation.replace(fromDir, destDir)
        val fileSuffix = if (fileName.contains("."))
          fileName.substring(fileName.lastIndexOf(".")) else ""
        val newFileName = mergeTime + "_" + num + fileSuffix

        destLocation = destLocation.substring(0, destLocation.lastIndexOf("/") + 1) + newFileName
        num = num + 1
        val destPath = new Path(destLocation)

        if (!fileSystem.exists(destPath.getParent)) {
          fileSystem.mkdirs(destPath.getParent)
        }
        fileSystem.rename(fromPath, destPath) // hdfs dfs -mv
      }

    })
  }

  private var accumulator: Accumulator[Int] = _

  /**
   * 全局自增key,数据平均分配
   *
   * @param partitions
   */
  class FairPartitioner(partitions: Int) extends Partitioner {
    require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

    def numPartitions: Int = partitions

    def getPartition(key: Any): Int = key match {
      case null => 0
      case _ => {
        accumulator.add(1)
        accumulator.value
      }
    }
  }


}
