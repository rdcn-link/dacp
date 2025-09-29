package link.rdcn.dacp.optree.fuse

import link.rdcn.Logging
import link.rdcn.struct.DefaultDataFrame

import java.nio.file.{Files, Path}
import java.util.concurrent.{CountDownLatch, TimeUnit}

object DataFrameMountUtils extends Logging {

  def mountDataFrameToTempPath(df: DefaultDataFrame, f: java.io.File => Unit): Unit = {
    //构造批次源和虚拟文件系统
    val batchSize = 100

    val batchSource = new RowBatchFSSource(df, batchSize)
    val fs = new RowBatchFS(batchSource)
    val mountLatch = new CountDownLatch(1)

    //创建临时挂载目录（确保该目录存在且为空）
    val mountDir = Files.createTempDirectory("fuse_mount_test")
    logger.info(s"Mount directory: $mountDir")

    //用线程挂载，避免阻塞主线程
    val mountThread = new Thread(() => {
      try {
        // 阻塞挂载，直到卸载
        fs.mount(mountDir, true)
        mountLatch.countDown()
        logger.info(s"Successfully mounted FUSE at $mountDir")
      } catch {
        case e: Exception =>
          e.printStackTrace()
          logger.error("Failed to mount FUSE file system!", e)
          mountLatch.countDown()
      }
    }, "FuseMountThread")

    mountThread.start()

    // 等待挂载生效（根据情况等待几秒）
    waitForMountReady(mountLatch)

    // 访问挂载目录，列出文件名并读取第一个批次文件内容打印
    val files = mountDir.toFile.listFiles()
    logger.info(s"Files in mount directory (${if (files != null) files.length else 0}):")

    //读取第一个批次文件内容并打印（按行读取，最多10行）
    val file = files.find(_.getName.startsWith("batch.json")).getOrElse(null)
    f(file)

    //卸载文件系统
    logger.info("\nUnmounting filesystem...")
    fs.umount()
    logger.info("Filesystem unmounted")

    // 删除临时目录
    mountDir.toFile.deleteOnExit()
  }

  private def waitForMountReady(mountLatch: CountDownLatch, timeoutSeconds: Int = 3): Unit = {
    if (!mountLatch.await(timeoutSeconds, TimeUnit.SECONDS))
      throw new RuntimeException(s"Mount directory is not ready or still empty after $timeoutSeconds seconds.")
  }
}
