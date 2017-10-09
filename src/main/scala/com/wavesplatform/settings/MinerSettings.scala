package com.wavesplatform.settings

import java.time.Duration

case class MinerSettings(
  enable: Boolean,
  quorum: Int,
  enableGrinding: Boolean,
  intervalAfterLastBlockThenGenerationIsAllowed: Duration)
