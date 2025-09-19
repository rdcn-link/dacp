package link.rdcn.client

import link.rdcn.TestEmptyProvider.outputDir
import link.rdcn.TestProvider
import link.rdcn.TestProvider.{baseDir, binDir, dc}
import link.rdcn.client.DataIntegrityTest.isFolderContentsMatch
import link.rdcn.struct.{Row, Blob}
import link.rdcn.util.DataUtils
import org.apache.commons.io.IOUtils
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.io.{FileOutputStream, InputStream}
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{Files, Path, Paths}
import java.security.MessageDigest

object DataIntegrityTest {
  def isFolderContentsMatch(dirPath1: String, dirPath2: String): Boolean = {
    val files1 = Files.list(Paths.get(dirPath1)).sorted.toArray
    val files2 = Files.list(Paths.get(dirPath2)).sorted.toArray
    files2.zip(files1).forall { case (f1, f2) =>
      computeFileHash(f1.asInstanceOf[Path]) == computeFileHash(f2.asInstanceOf[Path])
    }
  }

  def computeFileHash(file: Path, algorithm: String = "MD5"): String = {
    val digest = MessageDigest.getInstance(algorithm)
    val buffer = new Array[Byte](8192)
    var in: InputStream = null

    try {
      in = Files.newInputStream(file)
      var bytesRead = in.read(buffer)
      while (bytesRead != -1) {
        digest.update(buffer, 0, bytesRead)
        bytesRead = in.read(buffer)
      }
      digest.digest().map(b => f"${b & 0xff}%02x").mkString
    } finally {
      if (in != null) in.close() // 确保关闭
    }
  }
}

class DataIntegrityTest extends TestProvider {

  @ParameterizedTest
  @ValueSource(ints = Array(2))
  def readBinaryTest(num: Int): Unit = {
    val absolutePath = Paths.get(binDir, s"binary_data_$num.bin")
    val attrs = Files.readAttributes(absolutePath, classOf[BasicFileAttributes])
    val file = absolutePath.toFile
    val expectedRow: Row = {
      val temp = (
        file.getName, // name: String
        attrs.size(), // rowCount: Long
        DataUtils.getFileTypeByExtension(file), // 文件类型: String
        attrs.creationTime().toMillis, // 创建时间: Long (Millis)
        attrs.lastModifiedTime().toMillis, // 最后修改时间: Long (Millis)
        attrs.lastAccessTime().toMillis, // 最后访问时间: Long (Millis)
        file)
      Row.fromTuple(temp)
    }
    val expectedName = expectedRow.getAs[String](0)
    val expectedSize = expectedRow.getAs[Long](1)
    val expectedFileType = expectedRow.getAs[String](2)
    val expectedCreatedTime = expectedRow.getAs[Long](3)
    val expectedModifiedTime = expectedRow.getAs[Long](4)
    val expectedLastAccessTime = expectedRow.getAs[Long](5)

    val df = dc.getByPath("/bin")
    df.filter(row => row._1.asInstanceOf[String] == s"binary_data_$num.bin").foreach(
      row => {
        println(row)
        val name = row.getAs[String](0)
        val size = row.getAs[Long](1)
        val fileType = row.getAs[String](2)
        val createdTime = row.getAs[Long](3)
        val modifiedTime = row.getAs[Long](4)
        val lastAccessTime = row.getAs[Long](5)
        val blob = row.getAs[Blob](6)
        val path: Path = Paths.get("src", "test", "demo", "data", "output", name)
        blob.offerStream(inputStream => {
          val outputStream = new FileOutputStream(path.toFile)
          IOUtils.copy(inputStream, outputStream)
        })
        assertEquals(expectedName, name)
        assertEquals(expectedSize, size)
        assertEquals(expectedFileType, fileType)
        assertEquals(expectedCreatedTime, createdTime)
        assertEquals(expectedModifiedTime, modifiedTime)
        assertEquals(expectedLastAccessTime, lastAccessTime)
      }
    )
    assertTrue(isFolderContentsMatch(Paths.get(baseDir, "bin").toString, outputDir), "Binary file mismatch")
  }

}
