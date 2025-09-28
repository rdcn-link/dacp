package link.rdcn.dacp.struct

import link.rdcn.server.DftpTicket

/**
 * @Author renhao
 * @Description:
 * @Data 2025/9/20 15:24
 * @Modified By:
 */
case class CookTicket(ticketContent: String) extends DftpTicket {
  override val typeId: Byte = 3
}

