package link.rdcn.dacp.optree.fifo

import java.nio.file.{Paths, Files}
import java.io.{FileWriter, FileReader, BufferedReader, PrintWriter}

/**
 * @Author renhao
 * @Description:
 * @Data 2025/9/26 14:21
 * @Modified By:
 */
class NamedPipe(val path: String) {
  def create(): Unit = {
    if (!Files.exists(Paths.get(path))) {
      Runtime.getRuntime.exec(Array("mkfifo", path)).waitFor()
    }
  }

  def write(messages: Iterator[String]): Unit = {
    val writer = new PrintWriter(new FileWriter(path))
    try{
      messages.foreach(message => {
        writer.println(message)
        writer.flush()
      })
    } finally writer.close()
  }

  def read(): Iterator[String] = {
    new Iterator[String]{
      private val reader = new BufferedReader(new FileReader(path))
      private var nextLine: String = reader.readLine()
      private var isClosed = false

      override def hasNext: Boolean = {
        if (nextLine == null && !isClosed) {
          reader.close()
          isClosed = true
          false
        } else {
          nextLine != null
        }
      }

      override def next(): String = {
        if (!hasNext) throw new NoSuchElementException("No more lines")
        val current = nextLine
        nextLine = reader.readLine()
        if (nextLine == null) {
          reader.close()
          isClosed = true
        }
        current
      }
    }
  }

  def delete(): Unit = {
    Files.deleteIfExists(Paths.get(path))
  }
}

object NamedPipeExample extends App {
  val pipe = new NamedPipe("/tmp/scala_fifo")

  try {
    pipe.create()

    new Thread(()=>{
      val lines = pipe.read()
      lines.foreach(str => println(s"receive $str"))
    }).start()

    val dataToWrite = Iterator("Line 1", "Line 2", "Line 3")
    pipe.write(dataToWrite)

  } finally {
    pipe.delete()
  }
}
