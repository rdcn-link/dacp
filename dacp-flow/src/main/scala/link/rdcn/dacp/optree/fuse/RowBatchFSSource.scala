package link.rdcn.dacp.optree.fuse

import link.rdcn.struct.{ClosableIterator, DataFrame, Row}
import jnr.constants.platform.Errno
import jnr.ffi.Pointer
/**
 * @Author renhao
 * @Description: 懒加载批次流式读取DataFrame的Row
 * @Data 2025/7/30 17:30
 * @Modified By:
 */
class RowBatchFSSource(dataFrame: DataFrame, cacheSize: Int = 1024 * 1024 * 1024) {
  private val closableIterator: ClosableIterator[Row] = dataFrame.mapIterator(iter => iter)
  private val iterator: Iterator[String] = closableIterator.map(row => row.toJsonString(dataFrame.schema))
  private val cache = new Array[Byte](cacheSize)
  private var cacheStartOffset: Long = 0L
  private var cacheEndOffset: Long = 0L
  private var eofReached = false

  def read(buf: Pointer, size: Long, offset: Long): Int = synchronized {
    if (eofReached) {
      return -Errno.EIO.intValue()
    }
    val maxToRead = size.toInt

    if (offset < cacheStartOffset) {
      // 数据被覆盖，无法访问
      return -Errno.EIO.intValue()
    }

    val out = new Array[Byte](maxToRead)
    var written = 0
    var readOffset = offset

    while (written < maxToRead) {
      if (readOffset < cacheEndOffset) {
        // 从缓存读数据
        val cachePos = (readOffset - cacheStartOffset).toInt
        val available = (cacheEndOffset - readOffset).toInt
        if (available <= 0) {
          // 缓存没有更多数据，去读迭代器
        } else {
          val toCopy = math.min(maxToRead - written, available)
          System.arraycopy(cache, cachePos, out, written, toCopy)
          written += toCopy
          readOffset += toCopy
          // 如果这次已经填满缓冲区，就可以直接退出循环
          if (written >= maxToRead) {
            buf.put(0, out, 0, written)
            return written
          }
        }
      }

      // 从迭代器继续读数据
      if (!iterator.hasNext) {
        eofReached = true
        closableIterator.close()
        if (written == 0) return 0
        buf.put(0, out, 0, written)
        return written
      }

      val nextLine = iterator.next()

      val lineBytes = (nextLine + "\n").getBytes("UTF-8")

      // 缓存不足，丢弃最老数据
      if (cacheEndOffset + lineBytes.length - cacheStartOffset > cacheSize) {
        val overSize = (cacheEndOffset + lineBytes.length - cacheStartOffset - cacheSize).toInt
        cacheStartOffset += overSize
        System.arraycopy(cache, overSize, cache, 0, cacheSize - overSize)
      }

      val pos = (cacheEndOffset - cacheStartOffset).toInt
      System.arraycopy(lineBytes, 0, cache, pos, lineBytes.length)
      cacheEndOffset += lineBytes.length
    }

    buf.put(0, out, 0, written)
    written
  }
}