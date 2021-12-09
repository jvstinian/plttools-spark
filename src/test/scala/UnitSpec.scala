/*
 * MIT License
 * Copyright 2021 Justin Smith, jvstinian.com
*/
import org.scalatest._
import org.scalatest.flatspec.AnyFlatSpec

import matchers._

abstract class UnitSpec extends AnyFlatSpec with should.Matchers with OptionValues with Inside with Inspectors
