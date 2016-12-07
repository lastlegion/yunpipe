name: precompute
class: commandLineTool
baseCommand: [python, compute.py]
label: return the sum of input files
inputs:
    - id: inp
      type: FILE
      rename: input.txt
      required: true
outputs:
    - id: out
      type: FILE
      outputBinding: out.txt
hints:
    - container_name: wangyx2005/sum_ecs
    - system: ubuntu
    - work_dir: /tmp/work
    - memory: 128

    - instance_type: t2.micro
