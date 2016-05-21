package com.itegulov.dkvs.structure

import scala.language.implicitConversions
import scalaz.Scalaz._
import scalaz.{Order, Ordering}

sealed trait BallotNumber {
  def +(rhs: Int): BallotNumber
}

/**
  * @author Daniyar Itegulov
  */
case class BallotNumberReal(int: Int) extends BallotNumber {
  override def +(rhs: Int): BallotNumber = BallotNumberReal(int + rhs)
}

case class BallotNumberBottom() extends BallotNumber {
  override def +(rhs: Int): BallotNumber = BallotNumberReal(0)
}

object BallotNumber {
  val ⊥ = BallotNumberBottom()

  implicit def ballotNumberIsOrd: Order[BallotNumber] = new Order[BallotNumber] {
    override def order(x: BallotNumber, y: BallotNumber): Ordering = x match {
      case BallotNumberReal(a) => y match {
        case BallotNumberReal(b) => implicitly[Order[Int]].order(a, b)
        case BallotNumberBottom() => Ordering.GT
      }
      case BallotNumberBottom() => y match {
        case BallotNumberReal(_) => Ordering.LT
        case BallotNumberBottom() => Ordering.EQ
      }
    }
  }

  implicit def nonNegativeIntIsBallotNumber(number: Int): BallotNumber =
    BallotNumberReal(number)
}