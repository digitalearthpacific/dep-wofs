image: "mcr.microsoft.com/planetary-computer/python:latest"
profile: r
code: "src/"
command:
  - sh
  - run_on_pc.sh
  - mask.py
  - --datetime
  - "2013"
  - --version
  - "0.0.4"
