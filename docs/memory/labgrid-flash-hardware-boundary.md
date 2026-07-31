# Flash drivers cannot be exercised hardware-free — udev exports + exporter SSH

Learned: 2026-07-23 (re-verify if the labgrid pin moves past 26.x)

Unlike power (`rest` backend) and serial (`protocol: raw`), every flash-family
resource (DFU/fastboot/USBFlashable/IMXUSBLoader/USBMassStorage) is a
udev-managed USB export — there is no static-export route — and every driver op
SSHes to the exporter host (`NetworkResource.command_prefix`, ManagedFile
copies the image first). No localhost shortcut exists. How to apply: test the
job machinery with real subprocesses via scripted fake CLIs (per-thread
`processwrapper` callback demux, proven); treat real hardware as the e2e
boundary for the actual flash drivers and say so in docs. Details:
`docs/DESIGN.md` §11.11.
