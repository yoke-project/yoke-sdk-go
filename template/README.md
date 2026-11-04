# @YOKE_ID@

A Yoke Plugin written in Go.

| Path | What it holds |
| --- | --- |
| `yoke-plugin.yaml` | the file every step of `yoke-compose` reads: the identity, the build, the architectures |
| `plugin/` | the Plugin's source, and its Manifest, `@YOKE_ID@.yaml`, kept beside it |
| `try/` | an application project that uses the Plugin, to see it work |

Build it with `yoke-compose build`; publish it by tagging this repository `vX.Y.Z`.
