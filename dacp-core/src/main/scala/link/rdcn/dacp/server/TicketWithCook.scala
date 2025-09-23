package link.rdcn.dacp.server

import link.rdcn.server.{BlobTicket, DftpTicket, GetTicket}

import java.nio.charset.StandardCharsets

/**
 * @Author renhao
 * @Description:
 * @Data 2025/9/20 15:24
 * @Modified By:
 */
case class CookTicket(ticketContent: String) extends DftpTicket {
  override val typeId: Byte = 3
}

