inp = []

with open('input.txt') as f:
    for line in f:
        inp.append(int(line.strip()))

with open('out.txt', 'w') as f:
    f.write(str(sum(inp) / float(len(inp))) + '\n')
