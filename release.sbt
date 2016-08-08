import sbtrelease.ReleasePlugin.autoImport._
import ReleaseTransformations._

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies, // : ReleaseStep
  inquireVersions, // : ReleaseStep
  runTest, // : ReleaseStep
  setReleaseVersion, // : ReleaseStep
  commitReleaseVersion, // : ReleaseStep, performs the initial git checks
  tagRelease, // : ReleaseStep
  publishArtifacts, // : ReleaseStep, checks whether `publishTo` is properly set up
  pushChanges, // : ReleaseStep, also checks that an upstream branch is properly configured
  setNextVersion // : ReleaseStep
  // commitNextVersion,                    // : ReleaseStep
)
