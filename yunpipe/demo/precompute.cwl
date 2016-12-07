name: precompute
class: commandLineTool
baseCommand: [python, compute.py]
label: Example for one step, given a file where each line contains one number, outputs squares and cubes
inputs:
    - id: inp
      type: FILE
      rename: input.txt
      required: true
outputs:
    - id: sq
      type: FILE
      outputBinding: sq.txt
    - id: cb
      type: FILE
      outputBinding: cb.txt
hints:
    - container_name: wangyx2005/precompute_ecs
    - system: ubuntu
    - work_dir: /tmp/work

    - instance_type: t2.micro
    - memory:
        - minimal: 50
        - suggensted: 128
    - port:
        - port: 9090
          protocol: tcp
    - user_specified_environment_variables:
        - name: seeds
          required: true
