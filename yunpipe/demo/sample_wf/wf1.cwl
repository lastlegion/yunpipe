name: wf1
class: Workflow
inputs:
    - id: inp
      type: FILE
      required: yes
outputs:
    - id: out
      type: FILE
      source: "#precompute/sq"
steps:
    - id: precompute
      run: precompute.algorthm
      inputs:
        - id: inp
          source: "#inp"
      outputs:
        - id: sq
hints:
    - output_s3: "container-clouds-output"
    - intermediate_s3: "container-clouds-intermediate"