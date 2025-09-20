package link.rdcn.dacp.optree.fuse

import jnr.constants.platform.Errno
import jnr.ffi.Pointer
import ru.serce.jnrfuse.{FuseFillDir, FuseStubFS}
import ru.serce.jnrfuse.struct.{FileStat, FuseFileInfo}

import java.io.File

/**
 * @Author renhao
 * @Description: Iterator[String] 基于 jnr-fuse 的虚拟文件系统 列出batch.json等文件，按需read
 * @Data 2025/7/31 13:42
 * @Modified By:
 */

class RowBatchFS(batchSource: RowBatchFSSource) extends FuseStubFS {

  override def readdir(
                        path: String,
                        buf: Pointer,
                        filter: FuseFillDir,
                        offset: Long,
                        fi: FuseFileInfo
                      ): Int = {
    if (path == File.separator) {
      filter.apply(buf, ".", null, 0)
      filter.apply(buf, "..", null, 0)
      filter.apply(buf, "batch.json", null, 0)
      0
    } else {
      -Errno.ENOENT.intValue()
    }
  }

  override def getattr(path: String, stat: FileStat): Int = {
    if (path == File.separator) {
      stat.st_mode.set(FileStat.S_IFDIR | 0x555) // 只读目录权限
      stat.st_nlink.set(2)
      0
    } else {

      val name = path.stripPrefix(File.separator)
      if (name == "batch.json") {
        stat.st_mode.set(FileStat.S_IFREG | 0x444) // 只读文件权限
        stat.st_size.set(estimateFileSize(name)) // 估算文件大小，-1表示未知
        stat.st_nlink.set(1)
        0
      } else -Errno.ENOENT.intValue()
    }
  }

  override def read(
                     path: String,
                     buf: Pointer,
                     size: Long,
                     offset: Long,
                     fi: FuseFileInfo
                   ): Int = batchSource.read(buf, size, offset)

  private def estimateFileSize(batchName: String): Long = {
    // 数据大小未知，直接返回-1表示无法估计
    -1L
  }
}