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
  override val typeId: Byte = TicketWithCook.COOK_TICKET
}

object TicketWithCook{
  val BLOB_TICKET: Byte = 1
  val Get_TICKET: Byte = 2
  val COOK_TICKET: Byte = 3

  def decodeTicket(bytes: Array[Byte]): DftpTicket = {
    val buffer = java.nio.ByteBuffer.wrap(bytes)
    val typeId: Byte = buffer.get()
    val len = buffer.getInt()
    val b = new Array[Byte](len)
    buffer.get(b)
    val ticketContent = new String(b, StandardCharsets.UTF_8)
    typeId match {
      case BLOB_TICKET => BlobTicket(ticketContent)
      case Get_TICKET => GetTicket(ticketContent)
      case COOK_TICKET => CookTicket(ticketContent)
    }
  }
}

