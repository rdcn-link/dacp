package link.rdcn.dacp

/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/16 14:13
 * @Modified By:
 */
object ResourceKeys {
  // === CPU 资源 ===
  val CPU_CORES = "cpu.cores" // CPU核心数
  val CPU_USAGE_PERCENT = "cpu.usage.percent" // CPU使用率 (%)

  // === JVM 内存 ===
  val JVM_MAX_MEMORY_MB = "jvm.memory.max.mb" // 最大可用内存
  val JVM_TOTAL_MEMORY_MB = "jvm.memory.total.mb" // 已分配内存
  val JVM_USED_MEMORY_MB = "jvm.memory.used.mb" // 已使用内存
  val JVM_FREE_MEMORY_MB = "jvm.memory.free.mb" // 空闲内存

  // === 系统物理内存 ===
  val SYSTEM_MEMORY_TOTAL_MB = "system.memory.total.mb" // 总内存
  val SYSTEM_MEMORY_USED_MB = "system.memory.used.mb" // 已使用
  val SYSTEM_MEMORY_FREE_MB = "system.memory.free.mb" // 空闲
}
