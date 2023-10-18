image: "ghcr.io/digitalearthpacific/dep-mangroves:0.0.2-17-gfc0ade0"
profile: r
code: "src/"
command:
  - python
  - run_task.py
  - --region-code
  - "PG"
  - --region-index
  - "072"
  - --datetime
  - "2019"
  - --version
  - "test-6"
