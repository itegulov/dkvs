package com.itegulov.dkvs.structure

import java.util.UUID

/**
  * @author Daniyar Itegulov
  */
sealed trait Command {
  val clientId: UUID
}

case class SetCommand(key: String, value: String, clientId: UUID) extends Command

case class DeleteCommand(key: String, clientId: UUID) extends Command
