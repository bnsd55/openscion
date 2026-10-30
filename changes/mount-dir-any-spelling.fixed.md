`scn unmount DIR` finds a mount whatever way DIR is written: with a
`..` (`scn unmount ../view` from another directory) or through a
symlinked directory above it. The mount table compared paths as typed,
so such an unmount answered "no mount of scn is recorded", a second
`scn mount` of the same directory was not refused, and `scn commit
--from-mount` did not find the mount.
