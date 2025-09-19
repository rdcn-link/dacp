package link.rdcn.client

import link.rdcn.TestProvider
import link.rdcn.TestProvider.{csvDfInfos, dc}
import link.rdcn.struct.StructType.binaryStructType
import link.rdcn.struct.Blob
import link.rdcn.util.ServerUtils.convertStructTypeToArrowSchema
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertTrue}
import org.junit.jupiter.api.Test

class DataParsingTest extends TestProvider {

  @Test
  def CSVParsingTest(): Unit = {
    val df = dc.getByPath("/csv/data_1.csv")
    df.limit(1).foreach(
      row => {
        println(row)
        val rowLength = row.length
        val schemaLength = convertStructTypeToArrowSchema(csvDfInfos.head.schema).getFields.size

        assertEquals(schemaLength, rowLength, "Row length mismatch")

        assertNotNull(row.get(0), "field should not be null")
        assertNotNull(row.get(1), "field should not be null")

        assertTrue(row.get(0).isInstanceOf[Long], "field should be a Long")
        assertTrue(row.get(1).isInstanceOf[Double], "field should be a Double")

        val id: Long = row.getAs[Long](0)
        assertTrue(id >= 0, "id should be positive")
      }
    )
  }


  @Test
  def BinaryParsingTest(): Unit = {
    val df = dc.getByPath("/bin")
    df.limit(1).foreach(
      row => {
        println(row)
        val rowLength = row.length
        val schemaLength = convertStructTypeToArrowSchema(binaryStructType).getFields.size

        assertEquals(schemaLength, rowLength, "Row length mismatch")

        assertNotNull(row.get(0), "field should not be null")
        assertNotNull(row.get(1), "field should not be null")
        assertNotNull(row.get(2), "field should not be null")
        assertNotNull(row.get(3), "field should not be null")
        assertNotNull(row.get(4), "field should not be null")
        assertNotNull(row.get(5), "field should not be null")
        assertNotNull(row.get(6), "field should not be null")

        assertTrue(row.get(0).isInstanceOf[String], "field should be a String")
        assertTrue(row.get(1).isInstanceOf[Long], "field should be a Long")
        assertTrue(row.get(2).isInstanceOf[String], "field should be a String")
        assertTrue(row.get(3).isInstanceOf[Long], "field should be a Long")
        assertTrue(row.get(4).isInstanceOf[Long], "field should be a Long")
        assertTrue(row.get(5).isInstanceOf[Long], "field should be a Long")
        assertTrue(row.get(6).isInstanceOf[Blob], "field should be a Blob")

        val name = row.getAs[String](0)
        val size = row.getAs[Long](1)
        val fileType = row.getAs[String](2)
        val createdTime = row.getAs[Long](3)
        val modifiedTime = row.getAs[Long](4)
        val lastAccessTime = row.getAs[Long](5)

        assertTrue(name.nonEmpty, "name should not be empty")
        assertTrue(size >= 0, "byteSize should not be negative")
        assertTrue(fileType.nonEmpty, "fileType should not be empty")
        assertTrue(createdTime > 0, "time should be positive")
        assertTrue(modifiedTime > 0, "time should be positive")
        assertTrue(lastAccessTime > 0, "time should be positive")
      }
    )
  }

}
